mod auth;
mod exchange_reconcile;
mod operator_reports;
mod pipeline;
mod readiness;

use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use accounting::{
    compute_daily_pnl, compute_drawdown, mark_positions_to_market, PaperMarkPriceInput,
};
use aegis_core::{
    aggregate_closed_1m_candles, expected_testnet_pipeline_confirmation,
    expected_testnet_shadow_promotion_confirmation, is_valid_testnet_pipeline_confirmation,
    is_valid_testnet_shadow_promotion_confirmation, validate_testnet_repair_transition,
    AuthLoginRequest, AuthLoginResponse, AuthLogoutResponse, AuthRefreshResponse, AuthUserResponse,
    AuthenticatedActor, BacktestRequest, CandleAggregationRequest, CandleAggregationResult,
    CandleBackfillRequest, CandleBackfillResult, CandleInterval, EventEnvelope, ExchangeBalance,
    ExchangeCancelAck, ExchangeCancelRequest, ExchangeEnvironment, ExchangeName, ExchangeOrderAck,
    ExchangeOrderRequest, ExchangeOrderSide, ExchangeOrderTimeInForce, ExchangeOrderType,
    ExchangePrivateStreamSource, ExchangePrivateStreamState, ExchangePrivateStreamStatus,
    ExchangeRateLimitState, ExchangeReconciliationMismatch, ExchangeReconciliationRequest,
    ExchangeReconciliationResult, ExchangeReconciliationRun, ExchangeRequestMode,
    ExchangeSymbolInfo, ExchangeTestnetPipelinePreview, ExchangeTestnetPipelinePreviewRequest,
    ExchangeTestnetPipelineSubmitRequest, ExecutionReadinessRequest, ExecutionReadinessResult,
    ExecutionReadinessSnapshot, MarketCandleCoverageSummary, MarketMode, OperatorReport,
    OperatorReportRequest, OrderIntent, PaperCloseMode, PaperClosePositionRequest,
    PaperCloseReason, PaperPositionCloseSummary, PaperPositionStatusFilter, PaperPriceStatus,
    PaperTradingPipelineRequest, ResearchDataCoverageRequest, ResearchDataCoverageResult,
    ResearchDatasetBuildRequest, ResearchDatasetBuildResult, RiskCheckContext, RiskConfig,
    RiskConfigAuditEntry, RiskConfigValidationResult, RiskConfigVersion, RiskEvaluationDecision,
    RiskEvaluationResult, RiskRejectionReason, Side, SignalReason, StrategyComparisonSummary,
    StrategyConfig, StrategyConfigAuditEntry, StrategyConfigUpdateRequest,
    StrategyConfigValidationResult, StrategyConfigVersion, StrategyDataHealth,
    StrategyDecisionBreakdown, StrategyDiagnosticCheck, StrategyDiagnosticsDecision,
    StrategyDiagnosticsResult, StrategyDryRunRequest, StrategyDryRunResult,
    StrategyEvaluationContext, StrategyExperimentGlobalRanking, StrategyExperimentRequest,
    StrategyExperimentResult, StrategyExperimentRun, StrategyId,
    StrategyMultiTimeframeExperimentRequest, StrategyMultiTimeframeExperimentResult,
    StrategyNoSignalReason, StrategyPerformanceMode, StrategyPerformanceRequest,
    StrategyPerformanceSummary, StrategyPnlBreakdown, StrategyStatus, StrategyWalkForwardRequest,
    StrategyWalkForwardResult, StrategyWalkForwardWindowResult, Symbol, TestnetExecutionState,
    TestnetExecutionTransitionSource, TestnetPromotionFunnelRequest, TestnetPromotionFunnelRow,
    TestnetPromotionFunnelSummary, TestnetPromotionLifecycleBreakdown,
    TestnetPromotionOutcomeBreakdown, TestnetRepairAction, TestnetRepairActionStatus,
    TestnetRepairRequest, TestnetRepairResult, TestnetRepairValidationIssue,
    TestnetShadowPromotionPreview, TestnetShadowPromotionRequest, TestnetShadowPromotionResult,
    TestnetShadowPromotionStatus, TestnetShadowPromotionSubmitRequest, TestnetShadowRunRequest,
    TestnetShadowRunResult, TestnetShadowRunnerConfig, TestnetShadowRunnerConfigInput,
    TestnetShadowRunnerControlAction, TestnetShadowRunnerControlRequest, TestnetShadowRunnerState,
    TestnetShadowRunnerTickResult, UserRole, UserStatus,
};
use api::{
    close_paper_position, ensure_default_paper_account, persist_paper_fill_accounting,
    testnet_shadow::run_testnet_shadow_once,
    testnet_shadow_runner::{
        apply_testnet_shadow_runner_control_action, load_testnet_shadow_runner_snapshot,
        persist_testnet_shadow_runner_config, validate_testnet_shadow_runner_config,
        TestnetShadowRunnerConfigValidation,
    },
    AppConfig as ShadowAppConfig, AppState as ShadowAppState, ClosePaperPositionError,
    StrategyRuntimeConfig as ShadowStrategyRuntimeConfig,
};
use axum::{
    extract::{MatchedPath, Path, Query, Request, State},
    http::{
        header::{self, CONTENT_TYPE, COOKIE, SET_COOKIE, USER_AGENT},
        HeaderName, HeaderValue, Method, StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use db::{
    append_exchange_testnet_lifecycle_event_and_update_order, backtest_result_from_record,
    candle_backfill_result_from_record, check_health, connect_pool, count_users,
    create_paper_order, ensure_system_state, get_active_testnet_shadow_promotion_for_shadow_run,
    get_aggregated_candle_coverage, get_backtest_equity_curve, get_backtest_run,
    get_backtest_trades, get_candle_backfill_run, get_closed_1m_candles_range,
    get_default_paper_account, get_exchange_private_stream_state,
    get_exchange_testnet_order_by_client_order_id, get_latest_market_tick, get_order_by_id,
    get_paper_position_by_id, get_recent_closed_candles, get_risk_config, get_risk_decision_by_id,
    get_session_by_id, get_session_by_id_and_hash, get_strategy_backtest_breakdown,
    get_strategy_experiment, get_strategy_paper_pnl_breakdown, get_strategy_performance_summary,
    get_strategy_shadow_decision_breakdown, get_strategy_status, get_strategy_walk_forward_run,
    get_system_event, get_system_state, get_testnet_promotion_funnel_summary,
    get_testnet_promotion_lifecycle_breakdown, get_testnet_promotion_outcome_breakdown,
    get_testnet_shadow_promotion_by_id, get_testnet_shadow_run_by_id, get_user_by_email,
    get_user_by_id, insert_audit_log, insert_exchange_testnet_order,
    insert_exchange_testnet_repair_action, insert_paper_account, insert_paper_equity_snapshot,
    insert_risk_config_audit, insert_risk_evaluation, insert_session, insert_signal_deduped,
    insert_strategy_config_audit, insert_system_event, insert_testnet_shadow_promotion,
    insert_user, list_backtest_runs, list_candle_backfill_runs, list_candles,
    list_exchange_private_stream_events, list_exchange_reconciliation_mismatches,
    list_exchange_reconciliation_runs, list_exchange_testnet_order_lifecycle_events,
    list_exchange_testnet_orders, list_exchange_testnet_repair_actions, list_market_feed_statuses,
    list_open_paper_positions, list_orders, list_paper_equity_snapshots, list_paper_positions,
    list_paper_trade_journal, list_recent_risk_decisions_filtered, list_recent_signals,
    list_recent_system_events_filtered, list_risk_config_audit, list_risk_config_versions,
    list_strategy_config_audit, list_strategy_config_versions, list_strategy_experiment_runs,
    list_strategy_experiments, list_strategy_experiments_by_group,
    list_strategy_performance_rankings, list_strategy_status, list_strategy_walk_forward_runs,
    list_strategy_walk_forward_windows, list_testnet_promotion_funnel_rows,
    list_testnet_shadow_promotions, list_testnet_shadow_runs, load_risk_state_snapshot,
    paper_account_from_record, paper_equity_snapshot_from_record, paper_position_from_record,
    persist_risk_config_version, persist_strategy_config_version, revoke_session,
    risk_config_audit_from_record, risk_config_from_record, risk_config_version_from_record,
    rotate_session_refresh_token, set_kill_switch_state, strategy_config_audit_from_record,
    strategy_config_from_record, strategy_config_version_from_record,
    strategy_experiment_result_from_records, strategy_experiment_run_from_record,
    strategy_walk_forward_result_from_records, strategy_walk_forward_window_from_record,
    update_strategy_state, update_testnet_shadow_promotion_submission, update_user_last_login,
    upsert_aggregated_candles, upsert_exchange_private_stream_state, upsert_paper_position,
    upsert_risk_config, upsert_strategy_config, user_from_record, BacktestEquityPointRecord,
    BacktestTradeRecord, CandleBackfillRunRecord, CandleRecord, CreateOrderError, DbConfig,
    ExchangePrivateStreamEventRecord, ExchangePrivateStreamStateRecord,
    ExchangeTestnetOrderLifecycleEventRecord, ExchangeTestnetOrderRecord,
    ExchangeTestnetRepairActionRecord, InsertSignalOutcome, MarketFeedStatusRecord,
    MarketTickRecord, OrderRecord, PaperAccountRecord, PaperEquitySnapshotRecord,
    PaperPositionRecord, PaperTradeJournalRecord, PgPool, RiskDecisionRecord, SignalRecord,
    StateActor, StrategyStatusRecord, SystemEventRecord, SystemStateRecord,
    TestnetShadowPromotionRecord,
};
use events::{EventPublisher, PostgresEventPublisher, SystemEventType};
use exchange::{
    apply_testnet_transition, hash_listen_key, map_cancel_ack_to_transition,
    map_exchange_ack_to_transition, map_rest_reconciliation_status_to_transition, mask_listen_key,
    BinanceSpotTestnetAdapter, BinanceSpotTestnetConfig, BinanceTestnetStatus, ExchangeAdapter,
};
use market_ingest::{HistoricalCandleBackfillService, MarketIngestConfig, ResearchDatasetService};
use replay_engine::ReplayEngine;
use risk_engine::{validate_risk_config, RiskEvaluator};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use strategy_engine::{
    build_default_strategy_configs, diagnose as diagnose_strategy, evaluate as evaluate_strategy,
    required_candle_count, validate_strategy_config, StrategyValidationContext,
};
use telemetry::telemetry;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};
use uuid::Uuid;

use crate::auth::{
    actor_from_claims, bootstrap_credentials, build_refresh_cookie, clear_refresh_cookie,
    decode_access_token, dev_actor, dev_user, hash_password, hash_refresh_token,
    issue_access_token, issue_refresh_token, parse_refresh_token, verify_password, AuthConfig,
    REFRESH_COOKIE_NAME,
};
use crate::exchange_reconcile::{
    local_testnet_status_from_exchange_state, mismatch_from_record, reconcile_testnet_orders,
    run_from_record, run_result_from_run, ReconcileTestnetOrdersError,
};
use crate::operator_reports::OperatorReportListItem;

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
const DEFAULT_EXCHANGE_TESTNET_LIMIT: i64 = 20;
const MAX_EXCHANGE_TESTNET_LIMIT: i64 = 200;
const CLI_AUTH_MODE_HEADER: &str = "x-aegis-auth-mode";
const CLI_AUTH_MODE_VALUE: &str = "cli";
const TESTNET_ORDER_CONFIRMATION_TEXT: &str = "TESTNET ORDER";
const DEFAULT_TESTNET_SHADOW_PROMOTION_TTL_SECONDS: u64 = 300;
const DEFAULT_CORS_ALLOWED_ORIGINS: [&str; 2] = ["http://localhost:3001", "http://127.0.0.1:3001"];

#[derive(Debug, Clone)]
struct PreparedExchangeTestnetPipelinePreview {
    preview: ExchangeTestnetPipelinePreview,
    order: ExchangeOrderRequest,
}

#[derive(Clone)]
struct AppState {
    config: AppConfig,
    auth_config: AuthConfig,
    db_pool: PgPool,
    started_at: chrono::DateTime<Utc>,
    market_mode: MarketMode,
    market_config: MarketIngestConfig,
    strategy_runtime: StrategyRuntimeConfig,
    exchange_testnet_binance: Option<BinanceSpotTestnetAdapter>,
    exchange_testnet: Arc<dyn ExchangeAdapter>,
    exchange_testnet_environment: ExchangeEnvironment,
    exchange_testnet_status: BinanceTestnetStatus,
}

#[derive(Clone)]
struct AppConfig {
    app_name: String,
    environment: String,
    bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
    cors_allowed_origins: Vec<HeaderValue>,
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
        let cors_allowed_origins =
            parse_cors_allowed_origins(env::var("AEGIS_CORS_ALLOWED_ORIGINS").ok().as_deref())?;

        Ok(Self {
            app_name,
            environment,
            bind_addr,
            database_url,
            database_max_connections,
            cors_allowed_origins,
        })
    }
}

fn parse_cors_allowed_origins(value: Option<&str>) -> Result<Vec<HeaderValue>, String> {
    let configured = value
        .map(str::to_owned)
        .unwrap_or_else(|| DEFAULT_CORS_ALLOWED_ORIGINS.join(","));
    let mut origins = Vec::new();

    for candidate in configured
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if candidate == "*" {
            return Err(
                "AEGIS_CORS_ALLOWED_ORIGINS cannot contain '*' when credentialed auth is enabled"
                    .to_string(),
            );
        }
        if !candidate.starts_with("http://") && !candidate.starts_with("https://") {
            return Err(format!(
                "invalid AEGIS_CORS_ALLOWED_ORIGINS entry '{candidate}': expected http:// or https:// origin"
            ));
        }

        let header = HeaderValue::from_str(candidate).map_err(|err| {
            format!("invalid AEGIS_CORS_ALLOWED_ORIGINS entry '{candidate}': {err}")
        })?;

        if !origins.contains(&header) {
            origins.push(header);
        }
    }

    if origins.is_empty() {
        return Err("AEGIS_CORS_ALLOWED_ORIGINS must include at least one origin".to_string());
    }

    Ok(origins)
}

fn build_cors_layer(config: &AppConfig) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(config.cors_allowed_origins.clone()))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
        .allow_credentials(true)
}

#[derive(Clone)]
struct RequestContext {
    request_id: String,
    correlation_id: String,
}

#[derive(Debug, Deserialize)]
struct AuthRefreshRequest {
    refresh_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteAccess {
    Public,
    Authenticated,
    Operator,
    Owner,
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
struct StrategyExperimentsQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct StrategyAnalyticsQuery {
    strategy_id: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    mode: StrategyPerformanceMode,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct StrategyDecisionBreakdownQuery {
    symbol: Option<String>,
    timeframe: Option<String>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct TestnetPromotionFunnelQuery {
    strategy_id: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct OperatorReportsListQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct ExecutionReadinessSnapshotsQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct PaperListQuery {
    limit: Option<i64>,
    status: Option<String>,
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

fn state_actor_from_authenticated(actor: &AuthenticatedActor) -> StateActor {
    StateActor {
        actor: format!("user:{}", actor.email),
        actor_id: Some(actor.user_id),
    }
}

fn current_actor(extension: Option<Extension<AuthenticatedActor>>) -> Option<AuthenticatedActor> {
    extension.map(|value| value.0)
}

fn required_state_actor(extension: Option<Extension<AuthenticatedActor>>) -> StateActor {
    current_actor(extension)
        .map(|actor| state_actor_from_authenticated(&actor))
        .unwrap_or_else(|| StateActor::system("anonymous"))
}

fn shadow_runtime_state(state: &AppState) -> ShadowAppState {
    ShadowAppState {
        config: ShadowAppConfig {
            app_name: state.config.app_name.clone(),
            environment: state.config.environment.clone(),
            bind_addr: state.config.bind_addr,
            database_url: state.config.database_url.clone(),
            database_max_connections: state.config.database_max_connections,
        },
        db_pool: state.db_pool.clone(),
        started_at: state.started_at,
        market_mode: state.market_mode,
        market_config: state.market_config.clone(),
        strategy_runtime: ShadowStrategyRuntimeConfig {
            default_symbols: state.strategy_runtime.default_symbols.clone(),
            default_timeframe: state.strategy_runtime.default_timeframe,
            default_notional: state.strategy_runtime.default_notional,
            momentum_lookback_candles: state.strategy_runtime.momentum_lookback_candles,
            breakout_lookback_candles: state.strategy_runtime.breakout_lookback_candles,
        },
    }
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

#[derive(Serialize)]
struct RiskConfigView {
    config_id: Uuid,
    max_open_positions: i32,
    max_daily_loss_pct: String,
    max_weekly_loss_pct: String,
    max_position_notional: String,
    max_slippage_pct: String,
    max_consecutive_losses: i32,
    cooldown_seconds: i32,
    max_signal_age_ms: i64,
    stale_feed_threshold_seconds: i32,
    config_version: i32,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskConfigResponse {
    config: RiskConfigView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskConfigValidationResponse {
    validation: RiskConfigValidationResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskConfigVersionsResponse {
    versions: Vec<RiskConfigVersion>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskConfigAuditResponse {
    audit: Vec<RiskConfigAuditEntry>,
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
struct ExchangeTestnetStatusResponse {
    exchange: String,
    environment: String,
    rest_base_url: String,
    ws_base_url: String,
    configured: bool,
    request_mode: ExchangeRequestMode,
    rate_limits: ExchangeRateLimitState,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangePrivateStreamStatusResponse {
    state: ExchangePrivateStreamStateView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangePrivateStreamEventsResponse {
    events: Vec<ExchangePrivateStreamEventView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangePrivateStreamListenKeyResponse {
    state: ExchangePrivateStreamStateView,
    listen_key_status: String,
    listen_key_masked: Option<String>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetSymbolsResponse {
    symbols: Vec<ExchangeSymbolInfo>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetBalancesResponse {
    balances: Vec<ExchangeBalance>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetOrderResponse {
    order: ExchangeTestnetOrderView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct ExchangeTestnetPipelinePreviewResponse {
    preview: ExchangeTestnetPipelinePreview,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetPipelineSubmitResponse {
    preview: ExchangeTestnetPipelinePreview,
    order: ExchangeTestnetOrderView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetOrderLifecycleResponse {
    client_order_id: String,
    current_state: String,
    events: Vec<TestnetExecutionLifecycleEventView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetOrdersResponse {
    orders: Vec<ExchangeTestnetOrderView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetRepairResponse {
    client_order_id: String,
    action: TestnetRepairAction,
    status: TestnetRepairActionStatus,
    previous_state: Option<String>,
    next_state: Option<String>,
    correlation_id: Uuid,
    issues: Vec<TestnetRepairValidationIssue>,
    request_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetRepairsResponse {
    client_order_id: String,
    repairs: Vec<ExchangeTestnetRepairActionView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeReconciliationRunResponse {
    run: ExchangeReconciliationRun,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeReconciliationRunsResponse {
    runs: Vec<ExchangeReconciliationRun>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeReconciliationMismatchesResponse {
    mismatches: Vec<ExchangeReconciliationMismatch>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeReconciliationResultResponse {
    result: ExchangeReconciliationResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunResponse {
    run: TestnetShadowRunResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunsResponse {
    runs: Vec<TestnetShadowRunResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowPromotionResponse {
    promotion: TestnetShadowPromotionPreview,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowPromotionsResponse {
    promotions: Vec<TestnetShadowPromotionPreview>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowPromotionSubmitResponse {
    result: TestnetShadowPromotionResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunnerStatusResponse {
    config: TestnetShadowRunnerConfig,
    state: TestnetShadowRunnerState,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunnerConfigResponse {
    config: TestnetShadowRunnerConfig,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunnerConfigValidationResponse {
    validation: TestnetShadowRunnerConfigValidation,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetShadowRunnerControlResponse {
    state: TestnetShadowRunnerState,
    tick: Option<TestnetShadowRunnerTickResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct StrategyPerformanceSummaryResponse {
    summary: StrategyPerformanceSummary,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct StrategyPerformanceRankingsResponse {
    rankings: Vec<StrategyComparisonSummary>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct StrategyDecisionBreakdownResponse {
    breakdown: StrategyDecisionBreakdown,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct StrategyPnlBreakdownResponse {
    breakdown: StrategyPnlBreakdown,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetPromotionFunnelSummaryResponse {
    summary: TestnetPromotionFunnelSummary,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetPromotionFunnelOutcomesResponse {
    outcomes: Vec<TestnetPromotionOutcomeBreakdown>,
    lifecycle: Vec<TestnetPromotionLifecycleBreakdown>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TestnetPromotionFunnelRowsResponse {
    rows: Vec<TestnetPromotionFunnelRow>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct OperatorReportResponse {
    report: OperatorReport,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct OperatorReportsListResponse {
    reports: Vec<OperatorReportListItem>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct ExecutionReadinessResponse {
    readiness: ExecutionReadinessResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct ExecutionReadinessSnapshotsResponse {
    snapshots: Vec<ExecutionReadinessSnapshot>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetOrderView {
    id: Uuid,
    exchange: String,
    environment: String,
    client_order_id: String,
    exchange_order_id: Option<String>,
    symbol: String,
    side: String,
    order_type: String,
    time_in_force: Option<String>,
    requested_qty: Option<String>,
    requested_notional: Option<String>,
    limit_price: Option<String>,
    status: String,
    execution_state: String,
    last_transition_at: Option<chrono::DateTime<Utc>>,
    lifecycle_summary: ExchangeTestnetOrderLifecycleSummaryView,
    ack_payload: Option<Value>,
    latest_status_payload: Option<Value>,
    risk_decision_id: Option<Uuid>,
    created_by: Option<Uuid>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetOrderLifecycleSummaryView {
    current_state: String,
    total_events: usize,
    last_transition_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Serialize)]
struct TestnetExecutionLifecycleEventView {
    previous_state: Option<String>,
    next_state: String,
    transition_source: String,
    reason: Option<String>,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ExchangeTestnetRepairActionView {
    id: Uuid,
    client_order_id: String,
    action: String,
    status: String,
    previous_state: Option<String>,
    next_state: Option<String>,
    reason: Option<String>,
    payload: Option<Value>,
    actor_id: Option<Uuid>,
    created_at: chrono::DateTime<Utc>,
    correlation_id: Option<Uuid>,
}

#[derive(Serialize)]
struct ExchangePrivateStreamStateView {
    exchange: String,
    environment: String,
    status: String,
    listen_key_hash: Option<String>,
    connected_at: Option<chrono::DateTime<Utc>>,
    last_event_at: Option<chrono::DateTime<Utc>>,
    last_error: Option<String>,
    reconnect_count: i32,
    updated_at: chrono::DateTime<Utc>,
    is_stale: bool,
}

#[derive(Serialize)]
struct ExchangePrivateStreamEventView {
    id: Uuid,
    exchange: String,
    environment: String,
    source: String,
    event_type: String,
    symbol: Option<String>,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    execution_type: Option<String>,
    order_status: Option<String>,
    payload: Value,
    event_time: chrono::DateTime<Utc>,
    received_at: chrono::DateTime<Utc>,
    correlation_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct ExchangeTestnetOrdersQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct ExchangePrivateStreamEventsQuery {
    limit: Option<i64>,
    client_order_id: Option<String>,
    event_type: Option<String>,
}

#[derive(Deserialize)]
struct ExchangeReconciliationRunsQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct TestnetShadowRunsQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TestnetShadowPromotionsQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct ExchangePrivateStreamLifecycleRequest {
    listen_key: Option<String>,
    correlation_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct ReconcileExchangeTestnetOrdersRequest {
    limit: Option<i64>,
    status_filter: Option<Vec<String>>,
    correlation_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct SubmitExchangeTestnetOrderRequest {
    symbol: String,
    side: ExchangeOrderSide,
    order_type: ExchangeOrderType,
    time_in_force: Option<ExchangeOrderTimeInForce>,
    quantity: Option<String>,
    quote_notional: Option<String>,
    limit_price: Option<String>,
    risk_decision_id: Option<Uuid>,
    confirmation_text: String,
    recv_window_ms: Option<u64>,
    correlation_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct CancelExchangeTestnetOrderRequest {
    confirmation_text: String,
    recv_window_ms: Option<u64>,
    correlation_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct RepairExchangeTestnetOrderRequest {
    action: TestnetRepairAction,
    confirmation_text: String,
    reason: Option<String>,
    force: Option<bool>,
    correlation_id: Option<Uuid>,
    recv_window_ms: Option<u64>,
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

#[derive(Deserialize)]
struct CandleCoverageQuery {
    symbol: String,
}

#[derive(Deserialize)]
struct ResearchDataCoverageQuery {
    symbol: String,
    intervals: String,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    required_coverage_pct: Option<Decimal>,
    exchange: Option<String>,
}

#[derive(Deserialize)]
struct ResearchDatasetBuildsQuery {
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
struct CandleCoverageResponse {
    coverage: MarketCandleCoverageSummary,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ResearchDataCoverageResponse {
    coverage: ResearchDataCoverageResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ResearchDatasetBuildsResponse {
    builds: Vec<ResearchDatasetBuildResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ResearchDatasetBuildResponse {
    build: ResearchDatasetBuildResult,
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
struct StrategyExperimentRunAcceptedResponse {
    experiment: StrategyExperimentResult,
    runs: Vec<StrategyExperimentRun>,
}

#[derive(Serialize)]
struct StrategyMultiTimeframeExperimentAcceptedResponse {
    comparison: StrategyMultiTimeframeExperimentResult,
}

#[derive(Serialize)]
struct StrategyExperimentsResponse {
    experiments: Vec<StrategyExperimentResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyExperimentResponse {
    experiment: StrategyExperimentResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyExperimentRunsResponse {
    runs: Vec<StrategyExperimentRun>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyWalkForwardAcceptedResponse {
    walk_forward: StrategyWalkForwardResult,
    windows: Vec<StrategyWalkForwardWindowResult>,
}

#[derive(Serialize)]
struct StrategyWalkForwardRunsResponse {
    walk_forwards: Vec<StrategyWalkForwardResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyWalkForwardResponse {
    walk_forward: StrategyWalkForwardResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyWalkForwardWindowsResponse {
    windows: Vec<StrategyWalkForwardWindowResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyMultiTimeframeExperimentResponse {
    comparison: StrategyMultiTimeframeExperimentResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
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

#[derive(Deserialize)]
struct StrategyDiagnosticsQuery {
    symbol: Option<String>,
    timeframe: Option<String>,
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
struct StrategyConfigValidationResponse {
    validation: StrategyConfigValidationResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyConfigVersionsResponse {
    versions: Vec<StrategyConfigVersion>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyConfigAuditResponse {
    audit: Vec<StrategyConfigAuditEntry>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyDryRunResponse {
    result: StrategyDryRunResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyDiagnosticsResponse {
    result: StrategyDiagnosticsResult,
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

#[derive(Deserialize)]
struct PaperClosePositionPayload {
    confirmation_text: String,
    reason: Option<String>,
    close_mode: Option<String>,
    correlation_id: Option<Uuid>,
    allow_stale_price: Option<bool>,
}

#[derive(Serialize)]
struct PaperClosePositionResponse {
    status: String,
    position_id: Uuid,
    symbol: String,
    entry_price: String,
    exit_price: String,
    quantity: String,
    realized_pnl: String,
    fee: String,
    slippage_cost: String,
    close_fill_id: Uuid,
    journal_entry_id: Uuid,
    correlation_id: Uuid,
    closed_at: chrono::DateTime<Utc>,
    request_id: String,
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
    enabled: bool,
    mode: String,
    symbols: Vec<String>,
    timeframe: String,
    suggested_notional: String,
    max_signal_age_ms: i64,
    cooldown_seconds: i32,
    lookback_candles: i32,
    confidence_floor: Option<String>,
    stop_loss_pct: Option<String>,
    take_profit_pct: Option<String>,
    holding_candles: Option<i32>,
    notes: Option<String>,
    config_version: i32,
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
type RunStrategyExperimentRequest = StrategyExperimentRequest;
type RunStrategyWalkForwardRequest = StrategyWalkForwardRequest;
type RunStrategyMultiTimeframeExperimentRequest = StrategyMultiTimeframeExperimentRequest;

#[tokio::main]
async fn main() {
    init_tracing();

    let config = AppConfig::from_env().expect("invalid application configuration");
    let auth_config = AuthConfig::from_env().expect("invalid auth configuration");
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
    let exchange_testnet_adapter =
        BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig::from_env());
    let exchange_testnet_status = exchange_testnet_adapter.status();
    let exchange_testnet_environment = exchange_testnet_status.environment;
    let cors_layer = build_cors_layer(&config);

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
        auth_config: auth_config.clone(),
        db_pool,
        started_at,
        market_mode: MarketMode::Paper,
        market_config,
        strategy_runtime,
        exchange_testnet_binance: Some(exchange_testnet_adapter.clone()),
        exchange_testnet: Arc::new(exchange_testnet_adapter),
        exchange_testnet_environment,
        exchange_testnet_status,
    };

    let app = Router::new()
        .route("/system/health", get(health))
        .route("/system/status", get(status))
        .route("/system/db-health", get(db_health))
        .route("/metrics", get(metrics))
        .route("/auth/bootstrap-owner", post(bootstrap_owner))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/refresh", post(refresh))
        .route("/auth/me", get(me))
        .route("/events/recent", get(recent_events))
        .route("/events/:id", get(event_by_id))
        .route("/risk/status", get(risk_status))
        .route("/risk/decisions", get(get_risk_decisions))
        .route("/risk/decisions/:id", get(get_risk_decision))
        .route("/risk/kill-switch", post(enable_kill_switch))
        .route("/risk/resume", post(resume_trading))
        .route("/risk/config", get(get_risk_config_handler))
        .route("/risk/config/validate", post(validate_risk_config_handler))
        .route("/risk/config/update", post(update_risk_config_handler))
        .route(
            "/risk/config/versions",
            get(get_risk_config_versions_handler),
        )
        .route("/risk/config/audit", get(get_risk_config_audit_handler))
        .route("/risk/evaluate", post(evaluate_risk))
        .route("/exchange/testnet/status", get(get_exchange_testnet_status))
        .route(
            "/exchange/testnet/pipeline/preview",
            post(preview_exchange_testnet_pipeline),
        )
        .route(
            "/exchange/testnet/pipeline/submit",
            post(submit_exchange_testnet_pipeline),
        )
        .route(
            "/exchange/testnet/shadow/run",
            post(run_exchange_testnet_shadow_handler),
        )
        .route(
            "/exchange/testnet/shadow/runs",
            get(list_exchange_testnet_shadow_runs_handler),
        )
        .route(
            "/exchange/testnet/shadow/runs/:id",
            get(get_exchange_testnet_shadow_run_handler),
        )
        .route(
            "/exchange/testnet/shadow/promotions/preview",
            post(preview_exchange_testnet_shadow_promotion_handler),
        )
        .route(
            "/exchange/testnet/shadow/promotions",
            get(list_exchange_testnet_shadow_promotions_handler),
        )
        .route(
            "/exchange/testnet/shadow/promotions/:id",
            get(get_exchange_testnet_shadow_promotion_handler),
        )
        .route(
            "/exchange/testnet/shadow/promotions/:id/submit",
            post(submit_exchange_testnet_shadow_promotion_handler),
        )
        .route(
            "/exchange/testnet/shadow-runner/status",
            get(get_exchange_testnet_shadow_runner_status_handler),
        )
        .route(
            "/exchange/testnet/shadow-runner/config",
            get(get_exchange_testnet_shadow_runner_config_handler),
        )
        .route(
            "/exchange/testnet/shadow-runner/config/validate",
            post(validate_exchange_testnet_shadow_runner_config_handler),
        )
        .route(
            "/exchange/testnet/shadow-runner/config/update",
            post(update_exchange_testnet_shadow_runner_config_handler),
        )
        .route(
            "/exchange/testnet/shadow-runner/control",
            post(control_exchange_testnet_shadow_runner_handler),
        )
        .route(
            "/exchange/testnet/private-stream/status",
            get(get_exchange_testnet_private_stream_status),
        )
        .route(
            "/exchange/testnet/private-stream/events",
            get(list_exchange_testnet_private_stream_events),
        )
        .route(
            "/exchange/testnet/private-stream/listen-key",
            post(create_exchange_testnet_private_stream_listen_key),
        )
        .route(
            "/exchange/testnet/private-stream/listen-key/keepalive",
            post(keepalive_exchange_testnet_private_stream_listen_key),
        )
        .route(
            "/exchange/testnet/private-stream/listen-key/close",
            post(close_exchange_testnet_private_stream_listen_key),
        )
        .route(
            "/exchange/testnet/symbols",
            get(get_exchange_testnet_symbols),
        )
        .route(
            "/exchange/testnet/balances",
            get(get_exchange_testnet_balances),
        )
        .route(
            "/exchange/testnet/orders",
            get(list_exchange_testnet_orders_handler),
        )
        .route(
            "/exchange/testnet/orders",
            post(submit_exchange_testnet_order),
        )
        .route(
            "/exchange/testnet/orders/:client_order_id",
            get(get_exchange_testnet_order),
        )
        .route(
            "/exchange/testnet/orders/:client_order_id/lifecycle",
            get(get_exchange_testnet_order_lifecycle),
        )
        .route(
            "/exchange/testnet/orders/:client_order_id/cancel",
            post(cancel_exchange_testnet_order),
        )
        .route(
            "/exchange/testnet/orders/:client_order_id/repair",
            post(repair_exchange_testnet_order),
        )
        .route(
            "/exchange/testnet/orders/:client_order_id/repairs",
            get(list_exchange_testnet_order_repairs),
        )
        .route(
            "/exchange/testnet/reconcile",
            post(reconcile_exchange_testnet_orders_handler),
        )
        .route(
            "/exchange/testnet/reconciliation/runs",
            get(list_exchange_reconciliation_runs_handler),
        )
        .route(
            "/exchange/testnet/reconciliation/runs/:id",
            get(get_exchange_reconciliation_run_handler),
        )
        .route(
            "/exchange/testnet/reconciliation/runs/:id/mismatches",
            get(list_exchange_reconciliation_mismatches_handler),
        )
        .route("/paper/orders", post(create_order))
        .route("/paper/pipeline/run", post(run_paper_pipeline_handler))
        .route("/paper/account", get(get_paper_account))
        .route(
            "/paper/account/mark-to-market",
            post(mark_paper_account_to_market),
        )
        .route("/paper/positions", get(get_paper_positions))
        .route("/paper/positions/:id", get(get_paper_position))
        .route(
            "/paper/positions/:id/close",
            post(close_paper_position_handler),
        )
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
        .route(
            "/experiments/strategy/run",
            post(run_strategy_experiment_handler),
        )
        .route(
            "/experiments/strategy/multi-timeframe",
            post(run_multi_timeframe_strategy_experiment_handler),
        )
        .route(
            "/experiments/strategy/walk-forward",
            post(run_strategy_walk_forward_handler).get(list_strategy_walk_forward_runs_handler),
        )
        .route(
            "/experiments/strategy",
            get(list_strategy_experiments_handler),
        )
        .route(
            "/experiments/strategy/walk-forward/:id",
            get(get_strategy_walk_forward_handler),
        )
        .route(
            "/experiments/strategy/walk-forward/:id/windows",
            get(list_strategy_walk_forward_windows_handler),
        )
        .route(
            "/experiments/strategy/:id",
            get(get_strategy_experiment_handler),
        )
        .route(
            "/experiments/strategy/:id/comparison",
            get(get_strategy_multi_timeframe_comparison_handler),
        )
        .route(
            "/experiments/strategy/:id/runs",
            get(list_strategy_experiment_runs_handler),
        )
        .route(
            "/analytics/strategy/performance",
            get(get_strategy_performance_handler),
        )
        .route(
            "/analytics/strategy/rankings",
            get(list_strategy_performance_rankings_handler),
        )
        .route(
            "/analytics/strategy/:id/decision-breakdown",
            get(get_strategy_decision_breakdown_handler),
        )
        .route(
            "/analytics/strategy/:id/paper-pnl-breakdown",
            get(get_strategy_paper_pnl_breakdown_handler),
        )
        .route(
            "/analytics/strategy/:id/backtest-breakdown",
            get(get_strategy_backtest_breakdown_handler),
        )
        .route(
            "/analytics/testnet/promotion-funnel",
            get(get_testnet_promotion_funnel_handler),
        )
        .route(
            "/analytics/testnet/promotion-funnel/outcomes",
            get(get_testnet_promotion_funnel_outcomes_handler),
        )
        .route(
            "/analytics/testnet/promotion-funnel/rows",
            get(list_testnet_promotion_funnel_rows_handler),
        )
        .route(
            "/reports/operator/daily",
            post(generate_operator_report_handler),
        )
        .route("/reports/operator", get(list_operator_reports_handler))
        .route("/reports/operator/:id", get(get_operator_report_handler))
        .route("/readiness/check", post(check_execution_readiness_handler))
        .route(
            "/readiness/snapshots",
            get(list_execution_readiness_snapshots_handler),
        )
        .route(
            "/readiness/snapshots/:id",
            get(get_execution_readiness_snapshot_handler),
        )
        .route("/orders", get(get_orders))
        .route("/orders/:id", get(get_order))
        .route("/market/symbols", get(get_market_symbols))
        .route("/market/ticks/latest", get(get_latest_tick))
        .route("/market/candles", get(get_market_candles))
        .route("/market/candles/coverage", get(get_market_candle_coverage))
        .route(
            "/market/candles/aggregate",
            post(post_market_aggregate_candles),
        )
        .route(
            "/market/backfill/candles",
            post(post_market_backfill_candles),
        )
        .route("/market/backfill/runs", get(get_market_backfill_runs))
        .route("/market/backfill/runs/:id", get(get_market_backfill_run))
        .route("/market/feed-status", get(get_market_feed_status))
        .route("/research/data/coverage", get(get_research_data_coverage))
        .route("/research/data/build", post(post_research_data_build))
        .route("/research/data/builds", get(list_research_data_builds))
        .route("/research/data/builds/:id", get(get_research_data_build))
        .route("/strategy/list", get(get_strategy_list))
        .route("/strategy/:id/status", get(get_strategy_by_id))
        .route("/strategy/:id/config", get(get_strategy_config_handler))
        .route(
            "/strategy/:id/config/validate",
            post(validate_strategy_config_handler),
        )
        .route(
            "/strategy/:id/config/update",
            post(update_strategy_config_handler),
        )
        .route(
            "/strategy/:id/config/versions",
            get(get_strategy_config_versions_handler),
        )
        .route(
            "/strategy/:id/config/audit",
            get(get_strategy_config_audit_handler),
        )
        .route("/strategy/:id/enable", post(enable_strategy))
        .route("/strategy/:id/disable", post(disable_strategy))
        .route("/strategy/:id/evaluate", post(evaluate_strategy_handler))
        .route("/strategy/:id/dry-run", post(strategy_dry_run_handler))
        .route(
            "/strategy/:id/diagnostics",
            get(strategy_diagnostics_handler),
        )
        .route("/signals/recent", get(get_recent_signals))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_context_middleware,
        ))
        .layer(cors_layer)
        .with_state(state);

    info!(
        service = %config.app_name,
        environment = %config.environment,
        bind_addr = %config.bind_addr,
        db_max_connections = config.database_max_connections,
        auth_disabled = auth_config.disabled,
        cors_allowed_origins = ?config.cors_allowed_origins,
        "starting api server"
    );
    if auth_config.disabled {
        tracing::warn!(
            "AEGIS_AUTH_DISABLED=true; injecting local OWNER actor for all protected routes"
        );
    }

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

async fn request_context_middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let request_id = get_or_create_header(request.headers(), &REQUEST_ID_HEADER);
    let correlation_id = request
        .headers()
        .get(&CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| request_id.clone());

    let request_context = RequestContext {
        request_id: request_id.clone(),
        correlation_id: correlation_id.clone(),
    };
    request.extensions_mut().insert(request_context.clone());

    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .map(normalize_route_label)
        .unwrap_or_else(|| normalize_route_label(request.uri().path()));
    let mut response = match authorize_request(&state, &mut request, &request_context, &path).await
    {
        Ok(()) => next.run(request).await,
        Err(response) => response,
    };
    let duration = started_at.elapsed();

    telemetry().observe_api_request(
        method.as_str(),
        path.as_str(),
        response.status().as_u16(),
        duration,
    );

    if path == "/system/health" {
        telemetry().set_system_health(response.status().is_success());
    }
    if path == "/system/db-health" {
        telemetry().set_db_health(response.status().is_success());
    }

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
        latency_ms = duration.as_millis(),
        "request completed"
    );

    response
}

async fn authorize_request(
    state: &AppState,
    request: &mut Request,
    request_context: &RequestContext,
    path: &str,
) -> Result<(), Response> {
    let method = request.method().clone();
    let access = route_access(&method, path, state.auth_config.protect_metrics);

    if state.auth_config.disabled {
        if access != RouteAccess::Public {
            request.extensions_mut().insert(dev_actor());
        }
        return Ok(());
    }

    if access == RouteAccess::Public {
        return Ok(());
    }

    let Some(token) = bearer_token(request.headers()) else {
        return Err(auth_error_response(
            state,
            request_context,
            StatusCode::UNAUTHORIZED,
            "auth.unauthorized",
            "unauthorized",
            "Authentication is required.",
            None,
            path,
        )
        .await);
    };

    let claims = match decode_access_token(&state.auth_config, token) {
        Ok(claims) => claims,
        Err(_) => {
            return Err(auth_error_response(
                state,
                request_context,
                StatusCode::UNAUTHORIZED,
                "auth.unauthorized",
                "invalid_access_token",
                "Authentication is required.",
                None,
                path,
            )
            .await);
        }
    };

    let actor = match actor_from_claims(claims) {
        Ok(actor) => actor,
        Err(_) => {
            return Err(auth_error_response(
                state,
                request_context,
                StatusCode::UNAUTHORIZED,
                "auth.unauthorized",
                "invalid_access_token",
                "Authentication is required.",
                None,
                path,
            )
            .await);
        }
    };

    let Some(session_id) = actor.session_id else {
        return Err(auth_error_response(
            state,
            request_context,
            StatusCode::UNAUTHORIZED,
            "auth.unauthorized",
            "invalid_access_token",
            "Authentication is required.",
            None,
            path,
        )
        .await);
    };

    match get_session_by_id(&state.db_pool, session_id).await {
        Ok(Some(session)) if session.revoked_at.is_none() && session.expires_at > Utc::now() => {}
        _ => {
            return Err(auth_error_response(
                state,
                request_context,
                StatusCode::UNAUTHORIZED,
                "auth.unauthorized",
                "expired_or_revoked_session",
                "Authentication is required.",
                Some(&actor),
                path,
            )
            .await);
        }
    }

    let permitted = match access {
        RouteAccess::Public | RouteAccess::Authenticated => true,
        RouteAccess::Operator => actor.role == UserRole::Owner || actor.role == UserRole::Operator,
        RouteAccess::Owner => actor.role == UserRole::Owner,
    };

    if !permitted {
        return Err(auth_error_response(
            state,
            request_context,
            StatusCode::FORBIDDEN,
            "auth.forbidden",
            "forbidden",
            "You do not have permission to perform this action.",
            Some(&actor),
            path,
        )
        .await);
    }

    request.extensions_mut().insert(actor);
    Ok(())
}

async fn auth_error_response(
    state: &AppState,
    request: &RequestContext,
    status: StatusCode,
    event_type: &str,
    error_code: &'static str,
    message: &'static str,
    actor: Option<&AuthenticatedActor>,
    path: &str,
) -> Response {
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let payload = json!({
        "path": path,
        "status": status.as_u16(),
        "actor_id": actor.map(|value| value.user_id),
    });
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(event_type, correlation_id, &state.config.app_name, payload),
    )
    .await;

    if let Some(actor) = actor {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            event_type,
            path,
            &json!({ "status": status.as_u16(), "actor_id": actor.user_id }),
        )
        .await;
    }

    (
        status,
        Json(ErrorResponse {
            error: error_code,
            message: message.to_string(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

fn route_access(method: &axum::http::Method, path: &str, protect_metrics: bool) -> RouteAccess {
    if method == axum::http::Method::GET && path == "/system/health" {
        return RouteAccess::Public;
    }
    if method == axum::http::Method::POST
        && matches!(
            path,
            "/auth/login" | "/auth/bootstrap-owner" | "/auth/refresh"
        )
    {
        return RouteAccess::Public;
    }
    if method == axum::http::Method::GET && path == "/metrics" && !protect_metrics {
        return RouteAccess::Public;
    }
    if method == axum::http::Method::POST && path == "/reports/operator/daily" {
        return RouteAccess::Authenticated;
    }
    if method == axum::http::Method::GET
        && (path == "/reports/operator" || path.starts_with("/reports/operator/"))
    {
        return RouteAccess::Authenticated;
    }
    if method == axum::http::Method::GET && path == "/exchange/testnet/balances" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::GET
        && path.starts_with("/exchange/testnet/orders/")
        && path.ends_with("/repairs")
    {
        return RouteAccess::Authenticated;
    }
    if method == axum::http::Method::GET && path.starts_with("/exchange/testnet/orders/") {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::GET && path == "/exchange/testnet/orders" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/pipeline/preview" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/shadow/run" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/shadow/promotions/preview" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST
        && path.starts_with("/exchange/testnet/shadow/promotions/")
        && path.ends_with("/submit")
    {
        return RouteAccess::Owner;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/shadow-runner/config/update"
    {
        return RouteAccess::Owner;
    }
    if method == axum::http::Method::POST
        && path == "/exchange/testnet/shadow-runner/config/validate"
    {
        return RouteAccess::Owner;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/shadow-runner/control" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/pipeline/submit" {
        return RouteAccess::Owner;
    }
    if method == axum::http::Method::POST && path == "/exchange/testnet/reconcile" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST && path == "/market/candles/aggregate" {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::POST
        && path.starts_with("/exchange/testnet/orders/")
        && path.ends_with("/repair")
    {
        return RouteAccess::Operator;
    }
    if method == axum::http::Method::GET {
        return RouteAccess::Authenticated;
    }
    if path == "/risk/resume" || path == "/risk/config/update" {
        return RouteAccess::Owner;
    }
    if path.starts_with("/strategy/") && path.ends_with("/config/update") {
        return RouteAccess::Owner;
    }
    if path == "/exchange/testnet/orders" || path.starts_with("/exchange/testnet/orders/") {
        return RouteAccess::Owner;
    }
    RouteAccess::Operator
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

fn refresh_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value.split(';').find_map(|part| {
                let trimmed = part.trim();
                let (name, value) = trimmed.split_once('=')?;
                if name == REFRESH_COOKIE_NAME {
                    Some(value.to_string())
                } else {
                    None
                }
            })
        })
}

fn prefers_cli_auth_response(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(CLI_AUTH_MODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(CLI_AUTH_MODE_VALUE))
        .unwrap_or(false)
}

fn user_agent(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn request_ip(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned)
        })
}

fn get_or_create_header(headers: &axum::http::HeaderMap, name: &HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn normalize_route_label(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }

    let trimmed = path.trim();
    if trimmed == "/" {
        return "/".to_string();
    }

    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

async fn refresh_metrics_snapshot(state: &AppState) -> anyhow::Result<()> {
    let metrics = telemetry();

    match get_system_state(&state.db_pool).await {
        Ok(system_state) => metrics.set_kill_switch_active(system_state.kill_switch_enabled),
        Err(err) => {
            metrics.record_db_query_error("get_system_state");
            return Err(err.into());
        }
    }

    let now = Utc::now();
    match list_market_feed_statuses(&state.db_pool).await {
        Ok(feeds) => {
            let mut by_symbol = std::collections::BTreeMap::new();
            for feed in feeds {
                by_symbol.insert(feed.symbol.clone(), feed);
            }

            for symbol in &state.market_config.symbols {
                let symbol_str = symbol.as_str();
                let exchange = state.market_config.exchange.as_str();
                if let Some(feed) = by_symbol.get(symbol_str) {
                    let status = feed.status.to_ascii_lowercase();
                    let age_seconds = feed.last_event_at.map(|timestamp| {
                        now.signed_duration_since(timestamp).num_milliseconds() as f64 / 1000.0
                    });
                    metrics.set_market_feed_status(exchange, symbol_str, status.as_str());
                    metrics.set_market_feed_last_event_age_seconds(
                        exchange,
                        symbol_str,
                        age_seconds,
                    );
                } else {
                    metrics.set_market_feed_status(exchange, symbol_str, "unknown");
                    metrics.set_market_feed_last_event_age_seconds(exchange, symbol_str, None);
                }
            }
        }
        Err(err) => {
            metrics.record_db_query_error("list_market_feed_statuses");
            return Err(err.into());
        }
    }

    let paper_account = match get_default_paper_account(&state.db_pool).await {
        Ok(account) => account,
        Err(err) => {
            metrics.record_db_query_error("get_default_paper_account");
            return Err(err.into());
        }
    };

    match paper_account.as_ref() {
        Some(account) => metrics.set_paper_account_values(
            account.current_equity.to_f64().unwrap_or(0.0),
            account.realized_pnl.to_f64().unwrap_or(0.0),
            account.unrealized_pnl.to_f64().unwrap_or(0.0),
        ),
        None => metrics.set_paper_account_values(0.0, 0.0, 0.0),
    }

    match paper_account {
        Some(account) => match list_open_paper_positions(&state.db_pool, account.id).await {
            Ok(positions) => {
                let mut counts = std::collections::BTreeMap::<String, i64>::new();
                for position in positions {
                    *counts.entry(position.symbol).or_insert(0) += 1;
                }
                metrics.set_paper_positions_open(counts.into_iter());
            }
            Err(err) => {
                metrics.record_db_query_error("list_open_paper_positions");
                return Err(err.into());
            }
        },
        None => metrics.set_paper_positions_open(std::iter::empty()),
    }

    match check_health(&state.db_pool).await {
        Ok(()) => metrics.set_db_health(true),
        Err(err) => {
            metrics.record_db_query_error("check_health");
            metrics.set_db_health(false);
            return Err(err.into());
        }
    }

    Ok(())
}

fn request_context(request: Option<Extension<RequestContext>>) -> RequestContext {
    request
        .map(|Extension(value)| value)
        .unwrap_or(RequestContext {
            request_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        })
}

fn unauthorized_response(
    request: RequestContext,
    error: &'static str,
    message: &'static str,
) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            error,
            message: message.to_string(),
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

fn internal_error_response(
    error: &'static str,
    message: &'static str,
    request: RequestContext,
) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error,
            message: message.to_string(),
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
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

fn bounded_strategy_analytics_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(100),
        _ => 20,
    }
}

fn bounded_testnet_promotion_funnel_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(1000),
        _ => 100,
    }
}

fn strategy_performance_request_from_query(
    query: StrategyAnalyticsQuery,
) -> StrategyPerformanceRequest {
    StrategyPerformanceRequest {
        strategy_id: query.strategy_id,
        symbol: query.symbol,
        timeframe: query.timeframe,
        mode: query.mode,
        start_time: query.start_time,
        end_time: query.end_time,
        limit: Some(bounded_strategy_analytics_limit(query.limit)),
    }
}

fn testnet_promotion_funnel_request_from_query(
    query: TestnetPromotionFunnelQuery,
) -> TestnetPromotionFunnelRequest {
    TestnetPromotionFunnelRequest {
        strategy_id: query.strategy_id,
        symbol: query.symbol,
        timeframe: query.timeframe,
        start_time: query.start_time,
        end_time: query.end_time,
        limit: Some(bounded_testnet_promotion_funnel_limit(query.limit)),
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

fn bounded_exchange_testnet_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_EXCHANGE_TESTNET_LIMIT),
        _ => DEFAULT_EXCHANGE_TESTNET_LIMIT,
    }
}

fn bounded_exchange_reconciliation_runs_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_EXCHANGE_TESTNET_LIMIT),
        _ => DEFAULT_EXCHANGE_TESTNET_LIMIT,
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

async fn ensure_risk_config(state: &AppState) -> Result<db::RiskConfigRecord, anyhow::Error> {
    if let Some(record) = get_risk_config(&state.db_pool).await? {
        return Ok(record);
    }

    Ok(upsert_risk_config(&state.db_pool, &RiskConfig::default()).await?)
}

fn strategy_status_view(record: StrategyStatusRecord) -> StrategyStatusView {
    let state = record.state;
    StrategyStatusView {
        strategy_id: record.config.strategy_id,
        enabled: record.config.enabled,
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
        max_signal_age_ms: record.config.max_signal_age_ms,
        cooldown_seconds: record.config.cooldown_seconds,
        lookback_candles: record.config.lookback_candles,
        confidence_floor: record
            .config
            .confidence_floor
            .map(|value| value.to_string()),
        stop_loss_pct: record.config.stop_loss_pct.map(|value| value.to_string()),
        take_profit_pct: record.config.take_profit_pct.map(|value| value.to_string()),
        holding_candles: record.config.holding_candles,
        notes: record.config.notes,
        config_version: record.config.current_version,
        last_evaluated_at: state.as_ref().and_then(|state| state.last_evaluated_at),
        last_evaluation_reason: state
            .as_ref()
            .and_then(|state| state.last_evaluation_reason.clone()),
        last_signal_id: state.as_ref().and_then(|state| state.last_signal_id),
        last_signal_at: state.as_ref().and_then(|state| state.last_signal_at),
    }
}

fn strategy_update_request_from_config(config: &StrategyConfig) -> StrategyConfigUpdateRequest {
    StrategyConfigUpdateRequest {
        strategy_id: config.strategy_id.to_string(),
        enabled: config.enabled,
        mode: config.mode,
        symbols: config
            .symbols
            .iter()
            .map(|symbol| symbol.as_str().to_string())
            .collect(),
        timeframe: config.timeframe.as_str().to_string(),
        suggested_notional: config.suggested_notional,
        max_signal_age_ms: config.max_signal_age_ms,
        cooldown_seconds: config.cooldown_seconds,
        lookback_candles: config.lookback_candles,
        confidence_floor: config.confidence_floor,
        stop_loss_pct: config.stop_loss_pct,
        take_profit_pct: config.take_profit_pct,
        holding_candles: config.holding_candles,
        notes: config.notes.clone(),
    }
}

fn risk_config_view(record: &db::RiskConfigRecord) -> RiskConfigView {
    RiskConfigView {
        config_id: record.config_id,
        max_open_positions: record.max_open_positions,
        max_daily_loss_pct: record.max_daily_loss_pct.to_string(),
        max_weekly_loss_pct: record.max_weekly_loss_pct.to_string(),
        max_position_notional: record.max_position_notional.to_string(),
        max_slippage_pct: record.max_slippage_pct.to_string(),
        max_consecutive_losses: record.max_consecutive_losses,
        cooldown_seconds: record.cooldown_seconds,
        max_signal_age_ms: record.max_signal_age_ms,
        stale_feed_threshold_seconds: record.stale_feed_threshold_seconds,
        config_version: record.current_version,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn strategy_validation_context(state: &AppState) -> StrategyValidationContext {
    StrategyValidationContext {
        supported_symbols: state.market_config.symbols.clone(),
        max_position_notional: Some(aegis_core::RiskConfig::default().max_position_notional),
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

fn build_data_health(
    config: &StrategyConfig,
    candles: &[aegis_core::Candle],
    evaluated_at: DateTime<Utc>,
    limit: i64,
) -> StrategyDataHealth {
    let latest_closed_candle_time = candles.last().map(|candle| candle.close_time);
    let latest_closed_candle_age_ms = latest_closed_candle_time.map(|timestamp| {
        evaluated_at
            .signed_duration_since(timestamp)
            .num_milliseconds()
    });
    let stale = latest_closed_candle_age_ms
        .map(|age_ms| age_ms > config.max_signal_age_ms)
        .unwrap_or(false);

    StrategyDataHealth {
        required_lookback_candles: config.lookback_candles,
        required_closed_candles: required_candle_count(config),
        available_closed_candles: candles.len() as i64,
        latest_closed_candle_time,
        latest_closed_candle_age_ms,
        stale,
        latest_closes: candles
            .iter()
            .rev()
            .take(limit as usize)
            .map(|candle| format!("{} close={}", candle.close_time.to_rfc3339(), candle.close))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect(),
    }
}

fn is_valid_resume_confirmation(value: &str) -> bool {
    value.trim() == "RESUME TRADING"
}

fn is_valid_testnet_order_confirmation(value: &str) -> bool {
    value.trim() == TESTNET_ORDER_CONFIRMATION_TEXT
}

fn generate_testnet_client_order_id(correlation_id: Uuid) -> String {
    format!("aegis-testnet-{}", correlation_id.simple())
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

fn exchange_testnet_order_view(record: ExchangeTestnetOrderRecord) -> ExchangeTestnetOrderView {
    ExchangeTestnetOrderView {
        id: record.id,
        exchange: record.exchange,
        environment: record.environment,
        client_order_id: record.client_order_id,
        exchange_order_id: record.exchange_order_id,
        symbol: record.symbol,
        side: record.side,
        order_type: record.order_type,
        time_in_force: record.time_in_force,
        requested_qty: record.requested_qty.map(|value| value.to_string()),
        requested_notional: record.requested_notional.map(|value| value.to_string()),
        limit_price: record.limit_price.map(|value| value.to_string()),
        status: record.status,
        execution_state: record.execution_state.clone(),
        last_transition_at: record.last_transition_at,
        lifecycle_summary: ExchangeTestnetOrderLifecycleSummaryView {
            current_state: record.execution_state,
            total_events: 0,
            last_transition_at: record.last_transition_at,
        },
        ack_payload: record.ack_payload,
        latest_status_payload: record.latest_status_payload,
        risk_decision_id: record.risk_decision_id,
        created_by: record.created_by,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn lifecycle_event_view(
    record: ExchangeTestnetOrderLifecycleEventRecord,
) -> TestnetExecutionLifecycleEventView {
    TestnetExecutionLifecycleEventView {
        previous_state: record.previous_state,
        next_state: record.next_state,
        transition_source: record.transition_source,
        reason: record.reason,
        created_at: record.created_at,
    }
}

fn repair_action_view(
    record: ExchangeTestnetRepairActionRecord,
) -> ExchangeTestnetRepairActionView {
    ExchangeTestnetRepairActionView {
        id: record.id,
        client_order_id: record.client_order_id,
        action: record.action,
        status: record.status,
        previous_state: record.previous_state,
        next_state: record.next_state,
        reason: record.reason,
        payload: record.payload,
        actor_id: record.actor_id,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    }
}

fn parse_testnet_execution_state(value: &str) -> TestnetExecutionState {
    value.parse().unwrap_or(TestnetExecutionState::Failed)
}

async fn build_exchange_testnet_order_view(
    pool: &PgPool,
    record: ExchangeTestnetOrderRecord,
) -> anyhow::Result<ExchangeTestnetOrderView> {
    let events =
        list_exchange_testnet_order_lifecycle_events(pool, &record.client_order_id).await?;
    let mut view = exchange_testnet_order_view(record);
    view.lifecycle_summary.total_events = events.len();
    Ok(view)
}

async fn append_testnet_lifecycle_transition(
    pool: &PgPool,
    order: &ExchangeTestnetOrderRecord,
    next_state: TestnetExecutionState,
    source: TestnetExecutionTransitionSource,
    status: Option<&str>,
    exchange_order_id: Option<&str>,
    reason: Option<String>,
    payload: Option<Value>,
    created_by: Option<Uuid>,
    correlation_id: Option<Uuid>,
    is_ack_payload: bool,
) -> anyhow::Result<Option<ExchangeTestnetOrderRecord>> {
    let snapshot = aegis_core::TestnetOrderLifecycleSnapshot {
        order_id: Some(order.id),
        client_order_id: order.client_order_id.clone(),
        exchange_order_id: order.exchange_order_id.clone(),
        current_state: parse_testnet_execution_state(&order.execution_state),
        last_transition_at: order.last_transition_at,
    };
    let transition = apply_testnet_transition(
        &snapshot,
        next_state,
        source,
        reason.clone(),
        payload.clone(),
    )?;
    telemetry()
        .inc_exchange_testnet_lifecycle_transition(source.as_str(), transition.next_state.as_str());
    telemetry().apply_exchange_testnet_order_state_transition(
        transition.previous_state.map(|value| value.as_str()),
        transition.next_state.as_str(),
    );
    let event = ExchangeTestnetOrderLifecycleEventRecord {
        id: Uuid::new_v4(),
        order_id: Some(order.id),
        client_order_id: order.client_order_id.clone(),
        previous_state: transition
            .previous_state
            .map(|value| value.as_str().to_string()),
        next_state: transition.next_state.as_str().to_string(),
        transition_source: transition.source.as_str().to_string(),
        reason,
        payload: payload.clone(),
        created_by,
        created_at: Utc::now(),
        correlation_id,
    };
    append_exchange_testnet_lifecycle_event_and_update_order(
        pool,
        &event,
        exchange_order_id,
        status,
        transition.next_state,
        if is_ack_payload {
            None
        } else {
            payload.as_ref()
        },
        if is_ack_payload {
            payload.as_ref()
        } else {
            None
        },
    )
    .await
    .map_err(Into::into)
}

fn owner_required_for_repair(action: TestnetRepairAction, force: bool) -> bool {
    force || action.requires_owner()
}

fn operator_can_repair(action: TestnetRepairAction) -> bool {
    action.allows_operator()
}

fn is_testnet_repair_authorized(role: UserRole, action: TestnetRepairAction, force: bool) -> bool {
    if owner_required_for_repair(action, force) {
        role == UserRole::Owner
    } else {
        role == UserRole::Owner || (role == UserRole::Operator && operator_can_repair(action))
    }
}

fn order_has_cancelled_exchange_evidence(order: &ExchangeTestnetOrderRecord) -> bool {
    if order.status.eq_ignore_ascii_case("CANCELLED") {
        return true;
    }

    let has_cancel_status = |payload: &Value| {
        payload
            .get("status")
            .and_then(Value::as_str)
            .map(|value| {
                value.eq_ignore_ascii_case("CANCELED") || value.eq_ignore_ascii_case("CANCELLED")
            })
            .unwrap_or(false)
            || payload
                .get("X")
                .and_then(Value::as_str)
                .map(|value| {
                    value.eq_ignore_ascii_case("CANCELED")
                        || value.eq_ignore_ascii_case("CANCELLED")
                })
                .unwrap_or(false)
    };

    order
        .latest_status_payload
        .as_ref()
        .map(has_cancel_status)
        .unwrap_or(false)
        || order
            .ack_payload
            .as_ref()
            .map(has_cancel_status)
            .unwrap_or(false)
}

async fn persist_testnet_repair_action(
    pool: &PgPool,
    actor: &StateActor,
    client_order_id: &str,
    request: &TestnetRepairRequest,
    status: TestnetRepairActionStatus,
    previous_state: Option<TestnetExecutionState>,
    next_state: Option<TestnetExecutionState>,
    payload: Option<Value>,
    correlation_id: Uuid,
) -> anyhow::Result<ExchangeTestnetRepairActionRecord> {
    insert_exchange_testnet_repair_action(
        pool,
        &ExchangeTestnetRepairActionRecord {
            id: Uuid::new_v4(),
            client_order_id: client_order_id.to_string(),
            action: request.action.as_str().to_string(),
            status: status.as_str().to_string(),
            previous_state: previous_state.map(|value| value.as_str().to_string()),
            next_state: next_state.map(|value| value.as_str().to_string()),
            reason: request.reason.clone(),
            payload,
            actor_id: actor.actor_id,
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        },
    )
    .await
    .map_err(Into::into)
}

async fn append_explicit_testnet_repair_transition(
    pool: &PgPool,
    order: &ExchangeTestnetOrderRecord,
    action: TestnetRepairAction,
    next_state: TestnetExecutionState,
    source: TestnetExecutionTransitionSource,
    status: Option<&str>,
    exchange_order_id: Option<&str>,
    reason: Option<String>,
    payload: Option<Value>,
    created_by: Option<Uuid>,
    correlation_id: Uuid,
    force: bool,
) -> anyhow::Result<Option<ExchangeTestnetOrderRecord>> {
    let previous_state = parse_testnet_execution_state(&order.execution_state);
    validate_testnet_repair_transition(action, previous_state, Some(next_state), force)?;
    telemetry().inc_exchange_testnet_lifecycle_transition(source.as_str(), next_state.as_str());
    telemetry().apply_exchange_testnet_order_state_transition(
        Some(previous_state.as_str()),
        next_state.as_str(),
    );
    append_exchange_testnet_lifecycle_event_and_update_order(
        pool,
        &ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(order.id),
            client_order_id: order.client_order_id.clone(),
            previous_state: Some(previous_state.as_str().to_string()),
            next_state: next_state.as_str().to_string(),
            transition_source: source.as_str().to_string(),
            reason,
            payload: payload.clone(),
            created_by,
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        },
        exchange_order_id,
        status,
        next_state,
        payload.as_ref(),
        None,
    )
    .await
    .map_err(Into::into)
}

fn exchange_private_stream_state_view(
    record: ExchangePrivateStreamStateRecord,
) -> ExchangePrivateStreamStateView {
    let state = ExchangePrivateStreamState {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        status: record
            .status
            .parse()
            .unwrap_or(ExchangePrivateStreamStatus::Error),
        listen_key_hash: record.listen_key_hash.clone(),
        connected_at: record.connected_at,
        last_event_at: record.last_event_at,
        last_error: record.last_error.clone(),
        reconnect_count: record.reconnect_count,
        updated_at: record.updated_at,
    };
    let is_stale =
        exchange::private_stream_is_stale(&state, Utc::now(), std::time::Duration::from_secs(60));

    ExchangePrivateStreamStateView {
        exchange: record.exchange,
        environment: record.environment,
        status: record.status,
        listen_key_hash: record.listen_key_hash,
        connected_at: record.connected_at,
        last_event_at: record.last_event_at,
        last_error: record.last_error,
        reconnect_count: record.reconnect_count,
        updated_at: record.updated_at,
        is_stale,
    }
}

fn exchange_private_stream_event_view(
    record: ExchangePrivateStreamEventRecord,
) -> ExchangePrivateStreamEventView {
    ExchangePrivateStreamEventView {
        id: record.id,
        exchange: record.exchange,
        environment: record.environment,
        source: ExchangePrivateStreamSource::Websocket.as_str().to_string(),
        event_type: record.event_type,
        symbol: record.symbol,
        client_order_id: record.client_order_id,
        exchange_order_id: record.exchange_order_id,
        execution_type: record.execution_type,
        order_status: record.order_status,
        payload: record.payload,
        event_time: record.event_time,
        received_at: record.received_at,
        correlation_id: record.correlation_id,
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

fn paper_close_position_view(
    summary: PaperPositionCloseSummary,
    request_id: String,
) -> PaperClosePositionResponse {
    PaperClosePositionResponse {
        status: summary.status.as_str().to_ascii_uppercase(),
        position_id: summary.position_id,
        symbol: summary.symbol,
        entry_price: summary.entry_price.to_string(),
        exit_price: summary.exit_price.to_string(),
        quantity: summary.quantity.to_string(),
        realized_pnl: summary.realized_pnl.to_string(),
        fee: summary.fee.to_string(),
        slippage_cost: summary.slippage_cost.to_string(),
        close_fill_id: summary.close_fill_id,
        journal_entry_id: summary.journal_entry_id,
        correlation_id: summary.correlation_id,
        closed_at: summary.closed_at,
        request_id,
        timestamp: Utc::now(),
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
            exchange_execution: DependencyStatus {
                status: if state.exchange_testnet_status.configured {
                    "testnet_configured"
                } else {
                    "testnet_unconfigured"
                },
            },
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

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    telemetry().set_system_health(true);

    if let Err(err) = refresh_metrics_snapshot(&state).await {
        error!(error = %err, "failed to refresh telemetry snapshot gauges");
    }

    match telemetry().encode() {
        Ok(body) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => {
            error!(error = %err, "failed to encode prometheus metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(CONTENT_TYPE, "text/plain; charset=utf-8")],
                "failed to encode metrics".to_string(),
            )
                .into_response()
        }
    }
}

async fn bootstrap_owner(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);

    match count_users(&state.db_pool).await {
        Ok(count) if count > 0 => {
            return (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: "owner_already_bootstrapped",
                    message: "Bootstrap owner is only available before the first user exists."
                        .to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Ok(_) => {}
        Err(err) => {
            error!(error = %err, "failed to count users during bootstrap");
            return internal_error_response(
                "failed_to_bootstrap_owner",
                "Failed to inspect existing users before bootstrap.",
                request,
            );
        }
    }

    let (email, password) = match bootstrap_credentials(&state.auth_config) {
        Ok(credentials) => credentials,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "missing_bootstrap_credentials",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let password_hash = match hash_password(&password) {
        Ok(hash) => hash,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_bootstrap_password",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match insert_user(
        &state.db_pool,
        Uuid::new_v4(),
        &email,
        &password_hash,
        UserRole::Owner,
        UserStatus::Active,
    )
    .await
    {
        Ok(record) => {
            let user = match user_from_record(&record) {
                Ok(user) => user,
                Err(err) => {
                    error!(error = %err, "failed to map bootstrapped owner");
                    return internal_error_response(
                        "failed_to_bootstrap_owner",
                        "Owner was created but could not be loaded.",
                        request,
                    );
                }
            };
            let actor = StateActor {
                actor: format!("user:{}", user.email),
                actor_id: Some(user.id),
            };
            let metadata = json!({ "actor_id": user.id, "email": user.email, "role": user.role });
            let _ = insert_audit_log(
                &state.db_pool,
                correlation_id,
                &actor,
                "auth.owner_bootstrapped",
                "users/bootstrap-owner",
                &metadata,
            )
            .await;
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "auth.owner_bootstrapped",
                    correlation_id,
                    &state.config.app_name,
                    metadata,
                ),
            )
            .await;

            (StatusCode::CREATED, Json(AuthUserResponse { user })).into_response()
        }
        Err(err) => {
            error!(error = %err, "failed to bootstrap owner");
            internal_error_response(
                "failed_to_bootstrap_owner",
                "Owner bootstrap failed.",
                request,
            )
        }
    }
}

async fn login(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<AuthLoginRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let cli_auth = prefers_cli_auth_response(&headers);
    let email = payload.email.trim().to_ascii_lowercase();
    let user_agent = user_agent(&headers);
    let ip_address = request_ip(&headers);

    let record = match get_user_by_email(&state.db_pool, &email).await {
        Ok(record) => record,
        Err(err) => {
            error!(error = %err, "failed to query user during login");
            return internal_error_response(
                "failed_to_login",
                "Login failed due to a database error.",
                request,
            );
        }
    };

    let Some(record) = record else {
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "auth.login.failed",
                correlation_id,
                &state.config.app_name,
                json!({ "email": email }),
            ),
        )
        .await;
        return unauthorized_response(request, "invalid_credentials", "Invalid email or password.");
    };

    let user = match user_from_record(&record) {
        Ok(user) => user,
        Err(err) => {
            error!(error = %err, "failed to map user during login");
            return internal_error_response(
                "failed_to_login",
                "Login failed due to an internal mapping error.",
                request,
            );
        }
    };

    let password_ok = verify_password(&payload.password, &record.password_hash).unwrap_or(false);
    if !password_ok || user.status != UserStatus::Active {
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "auth.login.failed",
                correlation_id,
                &state.config.app_name,
                json!({ "email": email, "actor_id": user.id }),
            ),
        )
        .await;
        return unauthorized_response(request, "invalid_credentials", "Invalid email or password.");
    }

    let user = match update_user_last_login(&state.db_pool, user.id, Utc::now()).await {
        Ok(record) => match user_from_record(&record) {
            Ok(user) => user,
            Err(err) => {
                error!(error = %err, "failed to map user last login");
                return internal_error_response(
                    "failed_to_login",
                    "Login failed due to an internal mapping error.",
                    request,
                );
            }
        },
        Err(err) => {
            error!(error = %err, "failed to update last login");
            return internal_error_response(
                "failed_to_login",
                "Login failed due to a database error.",
                request,
            );
        }
    };

    let session_id = Uuid::new_v4();
    let refresh_token = issue_refresh_token(session_id);
    let refresh_expires_at = Utc::now()
        + chrono::Duration::from_std(state.auth_config.refresh_token_ttl)
            .expect("valid refresh ttl");
    if let Err(err) = insert_session(
        &state.db_pool,
        session_id,
        user.id,
        &refresh_token.hash,
        refresh_expires_at,
        user_agent.as_deref(),
        ip_address.as_deref(),
    )
    .await
    {
        error!(error = %err, "failed to create login session");
        return internal_error_response(
            "failed_to_login",
            "Login failed due to a session persistence error.",
            request,
        );
    }

    let access = match issue_access_token(&state.auth_config, &user, session_id, Utc::now()) {
        Ok(token) => token,
        Err(err) => {
            error!(error = %err, "failed to issue access token");
            return internal_error_response(
                "failed_to_login",
                "Login failed due to a token generation error.",
                request,
            );
        }
    };

    let actor = StateActor {
        actor: format!("user:{}", user.email),
        actor_id: Some(user.id),
    };
    let metadata = json!({ "actor_id": user.id, "email": user.email, "session_id": session_id });
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "auth.login.success",
        "auth/login",
        &metadata,
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "auth.login.success",
            correlation_id,
            &state.config.app_name,
            metadata,
        ),
    )
    .await;

    let mut response = (
        StatusCode::OK,
        Json(AuthLoginResponse {
            user,
            access_token: access.token,
            expires_at: access.expires_at,
            refresh_token: cli_auth.then_some(refresh_token.raw.clone()),
        }),
    )
        .into_response();
    if let Ok(cookie_value) = HeaderValue::from_str(
        &build_refresh_cookie(&state.auth_config, &refresh_token.raw).to_string(),
    ) {
        response.headers_mut().append(SET_COOKIE, cookie_value);
    }
    response
}

async fn logout(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = match current_actor(actor) {
        Some(actor) => actor,
        None => {
            return unauthorized_response(request, "unauthorized", "Authentication is required.");
        }
    };
    if let Some(session_id) = actor.session_id {
        let _ = revoke_session(&state.db_pool, session_id, Utc::now()).await;
    }
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let state_actor = state_actor_from_authenticated(&actor);
    let metadata = json!({ "actor_id": actor.user_id, "session_id": actor.session_id });
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &state_actor,
        "auth.logout",
        "auth/logout",
        &metadata,
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "auth.logout",
            correlation_id,
            &state.config.app_name,
            metadata,
        ),
    )
    .await;

    let mut response = (
        StatusCode::OK,
        Json(AuthLogoutResponse { logged_out: true }),
    )
        .into_response();
    if let Ok(cookie_value) =
        HeaderValue::from_str(&clear_refresh_cookie(&state.auth_config).to_string())
    {
        response.headers_mut().append(SET_COOKIE, cookie_value);
    }
    response
}

async fn refresh(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    headers: axum::http::HeaderMap,
    payload: Option<Json<AuthRefreshRequest>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let cli_auth = prefers_cli_auth_response(&headers);
    let refresh_token_from_body = payload
        .map(|Json(payload)| payload.refresh_token)
        .filter(|value| !value.trim().is_empty());
    let refresh_token_raw = refresh_cookie(&headers).or(refresh_token_from_body);
    let Some(refresh_token_raw) = refresh_token_raw else {
        return unauthorized_response(
            request,
            "missing_refresh_token",
            "Refresh token is required.",
        );
    };
    let session_id = match parse_refresh_token(&refresh_token_raw) {
        Ok(session_id) => session_id,
        Err(_) => {
            return unauthorized_response(
                request,
                "invalid_refresh_token",
                "Refresh token is invalid.",
            );
        }
    };
    let current_hash = hash_refresh_token(&refresh_token_raw);
    let Some(session) =
        (match get_session_by_id_and_hash(&state.db_pool, session_id, &current_hash).await {
            Ok(session) => session,
            Err(err) => {
                error!(error = %err, "failed to load refresh session");
                return internal_error_response(
                    "failed_to_refresh",
                    "Refresh failed due to a database error.",
                    request,
                );
            }
        })
    else {
        return unauthorized_response(
            request,
            "invalid_refresh_token",
            "Refresh token is invalid.",
        );
    };
    if session.revoked_at.is_some() || session.expires_at <= Utc::now() {
        return unauthorized_response(
            request,
            "expired_refresh_token",
            "Refresh token is expired.",
        );
    }

    let next_refresh_token = issue_refresh_token(session.id);
    let refresh_expires_at = Utc::now()
        + chrono::Duration::from_std(state.auth_config.refresh_token_ttl)
            .expect("valid refresh ttl");
    let rotated = match rotate_session_refresh_token(
        &state.db_pool,
        session.id,
        &current_hash,
        &next_refresh_token.hash,
        refresh_expires_at,
        user_agent(&headers).as_deref(),
        request_ip(&headers).as_deref(),
    )
    .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return unauthorized_response(
                request,
                "invalid_refresh_token",
                "Refresh token is invalid.",
            );
        }
        Err(err) => {
            error!(error = %err, "failed to rotate refresh session");
            return internal_error_response(
                "failed_to_refresh",
                "Refresh failed due to a database error.",
                request,
            );
        }
    };
    let user = match get_user_by_id(&state.db_pool, rotated.user_id).await {
        Ok(Some(record)) => match user_from_record(&record) {
            Ok(user) => user,
            Err(err) => {
                error!(error = %err, "failed to map user during refresh");
                return internal_error_response(
                    "failed_to_refresh",
                    "Refresh failed due to an internal mapping error.",
                    request,
                );
            }
        },
        Ok(None) => {
            return unauthorized_response(
                request,
                "invalid_refresh_token",
                "Refresh token is invalid.",
            );
        }
        Err(err) => {
            error!(error = %err, "failed to load user during refresh");
            return internal_error_response(
                "failed_to_refresh",
                "Refresh failed due to a database error.",
                request,
            );
        }
    };

    let access = match issue_access_token(&state.auth_config, &user, rotated.id, Utc::now()) {
        Ok(token) => token,
        Err(err) => {
            error!(error = %err, "failed to issue refreshed access token");
            return internal_error_response(
                "failed_to_refresh",
                "Refresh failed due to a token generation error.",
                request,
            );
        }
    };

    let actor = StateActor {
        actor: format!("user:{}", user.email),
        actor_id: Some(user.id),
    };
    let metadata = json!({ "actor_id": user.id, "session_id": rotated.id });
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "auth.refresh",
        "auth/refresh",
        &metadata,
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "auth.refresh",
            correlation_id,
            &state.config.app_name,
            metadata,
        ),
    )
    .await;

    let mut response = (
        StatusCode::OK,
        Json(AuthRefreshResponse {
            user,
            access_token: access.token,
            expires_at: access.expires_at,
            refresh_token: cli_auth.then_some(next_refresh_token.raw.clone()),
        }),
    )
        .into_response();
    if let Ok(cookie_value) = HeaderValue::from_str(
        &build_refresh_cookie(&state.auth_config, &next_refresh_token.raw).to_string(),
    ) {
        response.headers_mut().append(SET_COOKIE, cookie_value);
    }
    response
}

async fn me(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> impl IntoResponse {
    let _request = request_context(request);
    if state.auth_config.disabled {
        return (
            StatusCode::OK,
            Json(AuthUserResponse {
                user: dev_user(Utc::now()),
            }),
        )
            .into_response();
    }
    let Some(actor) = current_actor(actor) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "unauthorized",
                message: "Authentication is required.".to_string(),
                request_id: _request.request_id,
                correlation_id: _request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    };
    match get_user_by_id(&state.db_pool, actor.user_id).await {
        Ok(Some(record)) => match user_from_record(&record) {
            Ok(user) => (StatusCode::OK, Json(AuthUserResponse { user })).into_response(),
            Err(err) => {
                error!(error = %err, "failed to map /auth/me user");
                internal_error_response(
                    "failed_to_load_current_user",
                    "Failed to load the current user.",
                    _request,
                )
            }
        },
        Ok(None) => unauthorized_response(_request, "unauthorized", "Authentication is required."),
        Err(err) => {
            error!(error = %err, "failed to load /auth/me user");
            internal_error_response(
                "failed_to_load_current_user",
                "Failed to load the current user.",
                _request,
            )
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<KillSwitchRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let actor = required_state_actor(actor);

    match set_kill_switch_state(
        &state.db_pool,
        &actor,
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ResumeRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);

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
        &actor,
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

async fn get_risk_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match ensure_risk_config(&state).await {
        Ok(record) => (
            StatusCode::OK,
            Json(RiskConfigResponse {
                config: risk_config_view(&record),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_risk_config",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn validate_risk_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<RiskConfig>,
) -> impl IntoResponse {
    let request = request_context(request);
    let validation = validate_risk_config(&payload);
    telemetry().inc_risk_config_validation(if validation.valid {
        "valid"
    } else {
        "rejected"
    });
    let event_type = if validation.valid {
        "risk.config.validated"
    } else {
        "risk.config.rejected"
    };
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            parse_correlation_id(&request.correlation_id),
            state.config.app_name.clone(),
            json!({ "issues": validation.issues }),
        ),
    )
    .await;

    (
        if validation.valid {
            StatusCode::OK
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        },
        Json(RiskConfigValidationResponse {
            validation,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn update_risk_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<RiskConfig>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let actor = current_actor(actor);
    let actor_id = actor.as_ref().map(|value| value.user_id);
    let validation = validate_risk_config(&payload);
    let current_record = ensure_risk_config(&state).await.ok();
    let current_config = current_record
        .as_ref()
        .and_then(|record| risk_config_from_record(record).ok());
    telemetry().inc_risk_config_validation(if validation.valid {
        "valid"
    } else {
        "rejected"
    });

    if !validation.valid {
        telemetry().inc_risk_config_update("rejected");
        let config_id = current_record
            .as_ref()
            .map(|record| record.config_id)
            .unwrap_or_else(Uuid::new_v4);
        let _ = insert_risk_config_audit(
            &state.db_pool,
            &RiskConfigAuditEntry {
                audit_id: Uuid::new_v4(),
                config_id,
                version: None,
                old_config: current_config,
                new_config: None,
                validation_issues: validation.issues.clone(),
                actor_id,
                correlation_id,
                created_at: Utc::now(),
            },
        )
        .await;
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "risk.config.rejected",
                correlation_id,
                state.config.app_name.clone(),
                json!({ "issues": validation.issues, "actor_id": actor_id }),
            ),
        )
        .await;
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(RiskConfigValidationResponse {
                validation,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let config = validation
        .normalized_config
        .clone()
        .expect("valid config must be present");
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "risk.config.validated",
            correlation_id,
            state.config.app_name.clone(),
            json!({ "actor_id": actor_id }),
        ),
    )
    .await;

    match persist_risk_config_version(&state.db_pool, &config, actor_id, correlation_id).await {
        Ok(record) => {
            telemetry().inc_risk_config_update("updated");
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "risk.config.updated",
                    correlation_id,
                    state.config.app_name.clone(),
                    json!({
                        "config_id": record.config_id,
                        "version": record.current_version,
                        "actor_id": actor_id
                    }),
                ),
            )
            .await;
            (
                StatusCode::OK,
                Json(RiskConfigResponse {
                    config: risk_config_view(&record),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_update_risk_config",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_risk_config_versions_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_risk_config_versions(&state.db_pool).await {
        Ok(records) => (
            StatusCode::OK,
            Json(RiskConfigVersionsResponse {
                versions: records
                    .iter()
                    .map(risk_config_version_from_record)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_default(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_risk_config_versions",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_risk_config_audit_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_risk_config_audit(&state.db_pool).await {
        Ok(records) => (
            StatusCode::OK,
            Json(RiskConfigAuditResponse {
                audit: records
                    .iter()
                    .map(risk_config_audit_from_record)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_default(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_risk_config_audit",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
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
        RiskRejectionReason::MaxWeeklyLossExceeded => "max_weekly_loss_exceeded",
        RiskRejectionReason::MaxConsecutiveLossesExceeded => "max_consecutive_losses_exceeded",
        RiskRejectionReason::SignalTooOld => "signal_too_old",
        RiskRejectionReason::DuplicateOrderDetected => "duplicate_order_detected",
        RiskRejectionReason::DataStale => "data_stale",
        RiskRejectionReason::PositionNotionalExceeded => "position_notional_exceeded",
        RiskRejectionReason::CooldownActive => "cooldown_active",
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

    let risk_config = match ensure_risk_config(&state).await {
        Ok(record) => match risk_config_from_record(&record) {
            Ok(config) => config,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "invalid_persisted_risk_config",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        },
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_risk_config",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let evaluator = RiskEvaluator::new(risk_config);
    let evaluation = evaluator.evaluate(&context, &snapshot);
    telemetry().inc_risk_decision(
        match evaluation.decision {
            RiskEvaluationDecision::Approved => "approved",
            RiskEvaluationDecision::Rejected => "rejected",
        },
        evaluation
            .reasons
            .first()
            .map(|reason| reason_code(*reason))
            .unwrap_or("none"),
    );
    if evaluation.decision == RiskEvaluationDecision::Rejected {
        for reason in &evaluation.reasons {
            telemetry().inc_risk_rejection(reason_code(*reason));
        }
    }

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

async fn get_exchange_testnet_status(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let status = state.exchange_testnet_status.clone();

    (
        StatusCode::OK,
        Json(ExchangeTestnetStatusResponse {
            exchange: status.exchange.as_str().to_string(),
            environment: status.environment.as_str().to_string(),
            rest_base_url: status.rest_base_url,
            ws_base_url: status.ws_base_url,
            configured: status.configured,
            request_mode: status.request_mode,
            rate_limits: status.rate_limits,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
}

async fn get_exchange_testnet_private_stream_status(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_exchange_private_stream_state(
        &state.db_pool,
        ExchangeName::Binance.as_str(),
        ExchangeEnvironment::Testnet.as_str(),
    )
    .await
    {
        Ok(Some(record)) => {
            let view = exchange_private_stream_state_view(record);
            telemetry().set_exchange_private_stream_status(
                ExchangeEnvironment::Testnet.as_str(),
                &view.status,
            );
            let age_seconds = view
                .last_event_at
                .and_then(|value| Utc::now().signed_duration_since(value).to_std().ok())
                .map(|age| age.as_secs_f64())
                .unwrap_or(0.0);
            telemetry().set_exchange_private_stream_last_event_age_seconds(
                ExchangeEnvironment::Testnet.as_str(),
                age_seconds,
            );
            (
                StatusCode::OK,
                Json(ExchangePrivateStreamStatusResponse {
                    state: view,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Ok(None) => {
            telemetry().set_exchange_private_stream_status(
                ExchangeEnvironment::Testnet.as_str(),
                ExchangePrivateStreamStatus::Disconnected.as_str(),
            );
            telemetry().set_exchange_private_stream_last_event_age_seconds(
                ExchangeEnvironment::Testnet.as_str(),
                0.0,
            );
            (
                StatusCode::OK,
                Json(ExchangePrivateStreamStatusResponse {
                    state: exchange_private_stream_state_view(ExchangePrivateStreamStateRecord {
                        exchange: ExchangeName::Binance.as_str().to_string(),
                        environment: ExchangeEnvironment::Testnet.as_str().to_string(),
                        status: ExchangePrivateStreamStatus::Disconnected
                            .as_str()
                            .to_string(),
                        listen_key_hash: None,
                        connected_at: None,
                        last_event_at: None,
                        last_error: None,
                        reconnect_count: 0,
                        updated_at: Utc::now(),
                    }),
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
                "failed to load exchange private stream state"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_exchange_private_stream_state",
                    message: "Exchange private stream state could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn list_exchange_testnet_private_stream_events(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Query(query): Query<ExchangePrivateStreamEventsQuery>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = bounded_exchange_testnet_limit(query.limit);

    match list_exchange_private_stream_events(
        &state.db_pool,
        ExchangeEnvironment::Testnet.as_str(),
        limit,
        query.client_order_id.as_deref(),
        query.event_type.as_deref(),
    )
    .await
    {
        Ok(events) => (
            StatusCode::OK,
            Json(ExchangePrivateStreamEventsResponse {
                events: events
                    .into_iter()
                    .map(exchange_private_stream_event_view)
                    .collect(),
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
                "failed to list exchange private stream events"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_exchange_private_stream_events",
                    message: "Exchange private stream events could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn create_exchange_testnet_private_stream_listen_key(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ExchangePrivateStreamLifecycleRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    let Some(adapter) = state.exchange_testnet_binance.as_ref() else {
        return exchange_testnet_error_response(
            &request,
            "private_stream_create_listen_key",
            aegis_core::ExchangeError::Configuration(
                "binance private stream adapter unavailable".to_string(),
            ),
        );
    };

    match adapter.create_listen_key().await {
        Ok(created) => {
            let masked = mask_listen_key(&created.listen_key);
            let hashed = hash_listen_key(&created.listen_key);
            let persisted = upsert_exchange_private_stream_state(
                &state.db_pool,
                &ExchangePrivateStreamStateRecord {
                    exchange: created.exchange.as_str().to_string(),
                    environment: created.environment.as_str().to_string(),
                    status: ExchangePrivateStreamStatus::Disconnected
                        .as_str()
                        .to_string(),
                    listen_key_hash: Some(hashed),
                    connected_at: None,
                    last_event_at: None,
                    last_error: None,
                    reconnect_count: 0,
                    updated_at: Utc::now(),
                },
            )
            .await;

            match persisted {
                Ok(state_record) => {
                    let _ = insert_audit_log(
                        &state.db_pool,
                        correlation_id,
                        &actor,
                        "exchange.testnet.private_stream.listen_key.created",
                        ExchangeName::Binance.as_str(),
                        &json!({ "listen_key_masked": masked }),
                    )
                    .await;
                    (
                        StatusCode::OK,
                        Json(ExchangePrivateStreamListenKeyResponse {
                            state: exchange_private_stream_state_view(state_record),
                            listen_key_status: created.status.as_str().to_string(),
                            listen_key_masked: Some(masked),
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
                        "failed to persist exchange private stream state after listen key creation"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "failed_to_persist_exchange_private_stream_state",
                            message: "Exchange private stream state could not be persisted."
                                .to_string(),
                            request_id: request.request_id,
                            correlation_id: request.correlation_id,
                            timestamp: Utc::now(),
                        }),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            exchange_testnet_error_response(&request, "private_stream_create_listen_key", err)
        }
    }
}

async fn keepalive_exchange_testnet_private_stream_listen_key(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ExchangePrivateStreamLifecycleRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));
    let Some(listen_key) = payload.listen_key.filter(|value| !value.trim().is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "missing_listen_key",
                message: "listen_key is required for keepalive.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    };

    let Some(adapter) = state.exchange_testnet_binance.as_ref() else {
        return exchange_testnet_error_response(
            &request,
            "private_stream_keepalive_listen_key",
            aegis_core::ExchangeError::Configuration(
                "binance private stream adapter unavailable".to_string(),
            ),
        );
    };

    match adapter.keepalive_listen_key(&listen_key).await {
        Ok(keepalive) => match upsert_exchange_private_stream_state(
            &state.db_pool,
            &ExchangePrivateStreamStateRecord {
                exchange: keepalive.exchange.as_str().to_string(),
                environment: keepalive.environment.as_str().to_string(),
                status: ExchangePrivateStreamStatus::Disconnected
                    .as_str()
                    .to_string(),
                listen_key_hash: Some(hash_listen_key(&keepalive.listen_key)),
                connected_at: None,
                last_event_at: None,
                last_error: None,
                reconnect_count: 0,
                updated_at: Utc::now(),
            },
        )
        .await
        {
            Ok(state_record) => {
                let _ = insert_audit_log(
                    &state.db_pool,
                    correlation_id,
                    &actor,
                    "exchange.testnet.private_stream.listen_key.keepalive",
                    ExchangeName::Binance.as_str(),
                    &json!({ "listen_key_masked": mask_listen_key(&listen_key) }),
                )
                .await;
                (
                    StatusCode::OK,
                    Json(ExchangePrivateStreamListenKeyResponse {
                        state: exchange_private_stream_state_view(state_record),
                        listen_key_status: keepalive.status.as_str().to_string(),
                        listen_key_masked: Some(mask_listen_key(&listen_key)),
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
                    "failed to persist exchange private stream state after keepalive"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_persist_exchange_private_stream_state",
                        message: "Exchange private stream state could not be persisted."
                            .to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        },
        Err(err) => {
            exchange_testnet_error_response(&request, "private_stream_keepalive_listen_key", err)
        }
    }
}

async fn close_exchange_testnet_private_stream_listen_key(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ExchangePrivateStreamLifecycleRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));
    let Some(listen_key) = payload.listen_key.filter(|value| !value.trim().is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "missing_listen_key",
                message: "listen_key is required for close.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    };

    let Some(adapter) = state.exchange_testnet_binance.as_ref() else {
        return exchange_testnet_error_response(
            &request,
            "private_stream_close_listen_key",
            aegis_core::ExchangeError::Configuration(
                "binance private stream adapter unavailable".to_string(),
            ),
        );
    };

    match adapter.close_listen_key(&listen_key).await {
        Ok(closed) => match upsert_exchange_private_stream_state(
            &state.db_pool,
            &ExchangePrivateStreamStateRecord {
                exchange: closed.exchange.as_str().to_string(),
                environment: closed.environment.as_str().to_string(),
                status: ExchangePrivateStreamStatus::Disconnected
                    .as_str()
                    .to_string(),
                listen_key_hash: None,
                connected_at: None,
                last_event_at: None,
                last_error: None,
                reconnect_count: 0,
                updated_at: Utc::now(),
            },
        )
        .await
        {
            Ok(state_record) => {
                let _ = insert_audit_log(
                    &state.db_pool,
                    correlation_id,
                    &actor,
                    "exchange.testnet.private_stream.listen_key.closed",
                    ExchangeName::Binance.as_str(),
                    &json!({ "listen_key_masked": mask_listen_key(&listen_key) }),
                )
                .await;
                (
                    StatusCode::OK,
                    Json(ExchangePrivateStreamListenKeyResponse {
                        state: exchange_private_stream_state_view(state_record),
                        listen_key_status: closed.status.as_str().to_string(),
                        listen_key_masked: Some(mask_listen_key(&listen_key)),
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
                    "failed to persist exchange private stream state after close"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_persist_exchange_private_stream_state",
                        message: "Exchange private stream state could not be persisted."
                            .to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        },
        Err(err) => {
            exchange_testnet_error_response(&request, "private_stream_close_listen_key", err)
        }
    }
}

async fn get_exchange_testnet_symbols(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_exchange_testnet_request("symbols", "attempt");

    match state.exchange_testnet.get_exchange_info().await {
        Ok(symbols) => {
            telemetry().inc_exchange_testnet_request("symbols", "ok");
            (
                StatusCode::OK,
                Json(ExchangeTestnetSymbolsResponse {
                    symbols,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => exchange_testnet_error_response(&request, "symbols", err),
    }
}

async fn get_exchange_testnet_balances(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_exchange_testnet_request("balances", "attempt");

    match state.exchange_testnet.get_balances().await {
        Ok(balances) => {
            telemetry().inc_exchange_testnet_request("balances", "ok");
            (
                StatusCode::OK,
                Json(ExchangeTestnetBalancesResponse {
                    balances,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => exchange_testnet_error_response(&request, "balances", err),
    }
}

async fn list_exchange_testnet_orders_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Query(query): Query<ExchangeTestnetOrdersQuery>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = bounded_exchange_testnet_limit(query.limit);

    match list_exchange_testnet_orders(&state.db_pool, limit).await {
        Ok(orders) => {
            let mut views = Vec::with_capacity(orders.len());
            for order in orders {
                match build_exchange_testnet_order_view(&state.db_pool, order).await {
                    Ok(view) => views.push(view),
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_build_exchange_testnet_order_view",
                                message: err.to_string(),
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
                Json(ExchangeTestnetOrdersResponse {
                    orders: views,
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
                "failed to list exchange testnet orders"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_exchange_testnet_orders",
                    message: "Exchange testnet orders could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_exchange_testnet_order(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(client_order_id): Path<String>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_exchange_testnet_order_by_client_order_id(&state.db_pool, &client_order_id).await {
        Ok(Some(order)) => {
            telemetry().inc_exchange_testnet_request("get_order_status", "attempt");
            match state
                .exchange_testnet
                .get_order_status(&client_order_id)
                .await
            {
                Ok(status) => {
                    telemetry().inc_exchange_testnet_request("get_order_status", "ok");
                    let payload = status.raw_payload.clone();
                    let (next_state, reason) =
                        map_rest_reconciliation_status_to_transition(&status);
                    match append_testnet_lifecycle_transition(
                        &state.db_pool,
                        &order,
                        next_state,
                        TestnetExecutionTransitionSource::RestReconciliation,
                        Some(status.status.as_str()),
                        status.exchange_order_id.as_deref(),
                        reason.map(ToString::to_string),
                        Some(payload),
                        None,
                        Some(parse_correlation_id(&request.correlation_id)),
                        false,
                    )
                    .await
                    {
                        Ok(Some(updated)) => {
                            let view = build_exchange_testnet_order_view(&state.db_pool, updated)
                                .await
                                .unwrap_or_else(|_| exchange_testnet_order_view(order.clone()));
                            (
                                StatusCode::OK,
                                Json(ExchangeTestnetOrderResponse {
                                    order: view,
                                    request_id: request.request_id,
                                    correlation_id: request.correlation_id,
                                    timestamp: Utc::now(),
                                }),
                            )
                                .into_response()
                        }
                        Ok(None) | Err(_) => {
                            let view =
                                build_exchange_testnet_order_view(&state.db_pool, order.clone())
                                    .await
                                    .unwrap_or_else(|_| exchange_testnet_order_view(order.clone()));
                            (
                                StatusCode::OK,
                                Json(ExchangeTestnetOrderResponse {
                                    order: view,
                                    request_id: request.request_id,
                                    correlation_id: request.correlation_id,
                                    timestamp: Utc::now(),
                                }),
                            )
                                .into_response()
                        }
                    }
                }
                Err(err) => {
                    if matches!(err, aegis_core::ExchangeError::Configuration(_)) {
                        let view = build_exchange_testnet_order_view(&state.db_pool, order.clone())
                            .await
                            .unwrap_or_else(|_| exchange_testnet_order_view(order));
                        (
                            StatusCode::OK,
                            Json(ExchangeTestnetOrderResponse {
                                order: view,
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response()
                    } else {
                        exchange_testnet_error_response(&request, "get_order_status", err)
                    }
                }
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "exchange_testnet_order_not_found",
                message: "Exchange testnet order was not found.".to_string(),
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
                "failed to query exchange testnet order"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_exchange_testnet_order",
                    message: "Exchange testnet order could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_exchange_testnet_order_lifecycle(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(client_order_id): Path<String>,
) -> impl IntoResponse {
    let request = request_context(request);
    match get_exchange_testnet_order_by_client_order_id(&state.db_pool, &client_order_id).await {
        Ok(Some(order)) => {
            match list_exchange_testnet_order_lifecycle_events(&state.db_pool, &client_order_id)
                .await
            {
                Ok(events) => (
                    StatusCode::OK,
                    Json(ExchangeTestnetOrderLifecycleResponse {
                        client_order_id,
                        current_state: order.execution_state,
                        events: events.into_iter().map(lifecycle_event_view).collect(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_list_exchange_testnet_order_lifecycle",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "exchange_testnet_order_not_found",
                message: "Exchange testnet order was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_exchange_testnet_order",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_exchange_testnet_order_repairs(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(client_order_id): Path<String>,
) -> impl IntoResponse {
    let request = request_context(request);
    match list_exchange_testnet_repair_actions(&state.db_pool, &client_order_id).await {
        Ok(repairs) => (
            StatusCode::OK,
            Json(ExchangeTestnetRepairsResponse {
                client_order_id,
                repairs: repairs.into_iter().map(repair_action_view).collect(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_list_exchange_testnet_repairs",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn repair_exchange_testnet_order(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Path(client_order_id): Path<String>,
    Json(payload): Json<RepairExchangeTestnetOrderRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let authenticated_actor = current_actor(actor.clone());
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));
    let repair_request = TestnetRepairRequest {
        action: payload.action,
        confirmation_text: payload.confirmation_text,
        reason: payload.reason,
        force: payload.force.unwrap_or(false),
        correlation_id: Some(correlation_id),
    };

    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "exchange.testnet.repair.requested",
        &client_order_id,
        &json!({
            "action": repair_request.action.as_str(),
            "force": repair_request.force,
        }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.repair.requested",
            correlation_id,
            &state.config.app_name,
            json!({
                "client_order_id": client_order_id,
                "action": repair_request.action.as_str(),
                "force": repair_request.force,
            }),
        ),
    )
    .await;

    let Some(authenticated_actor) = authenticated_actor else {
        telemetry()
            .inc_exchange_testnet_repair_rejection(repair_request.action.as_str(), "missing_actor");
        return unauthorized_response(
            request.clone(),
            "unauthorized",
            "Authentication is required.",
        );
    };

    let permitted = is_testnet_repair_authorized(
        authenticated_actor.role,
        repair_request.action,
        repair_request.force,
    );
    if !permitted {
        let _ = persist_testnet_repair_action(
            &state.db_pool,
            &actor,
            &client_order_id,
            &repair_request,
            TestnetRepairActionStatus::Rejected,
            None,
            None,
            Some(json!({ "reason": "forbidden" })),
            correlation_id,
        )
        .await;
        telemetry().inc_exchange_testnet_repair(
            repair_request.action.as_str(),
            TestnetRepairActionStatus::Rejected.as_str(),
        );
        telemetry()
            .inc_exchange_testnet_repair_rejection(repair_request.action.as_str(), "forbidden");
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.repair.rejected",
                correlation_id,
                &state.config.app_name,
                json!({ "client_order_id": client_order_id, "action": repair_request.action.as_str(), "reason": "forbidden" }),
            ),
        )
        .await;
        return unauthorized_response(
            request.clone(),
            "forbidden",
            "You do not have permission to perform this repair action.",
        );
    }

    if let Err(err) = repair_request.validate_confirmation(&client_order_id) {
        let issues = vec![TestnetRepairValidationIssue {
            code: "invalid_confirmation".to_string(),
            message: err.to_string(),
        }];
        let _ = persist_testnet_repair_action(
            &state.db_pool,
            &actor,
            &client_order_id,
            &repair_request,
            TestnetRepairActionStatus::Rejected,
            None,
            None,
            Some(json!({ "issues": issues })),
            correlation_id,
        )
        .await;
        telemetry().inc_exchange_testnet_repair(
            repair_request.action.as_str(),
            TestnetRepairActionStatus::Rejected.as_str(),
        );
        telemetry().inc_exchange_testnet_repair_rejection(
            repair_request.action.as_str(),
            "invalid_confirmation",
        );
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.repair.rejected",
                correlation_id,
                &state.config.app_name,
                json!({ "client_order_id": client_order_id, "action": repair_request.action.as_str(), "reason": "invalid_confirmation" }),
            ),
        )
        .await;
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_testnet_repair_confirmation",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let order =
        match get_exchange_testnet_order_by_client_order_id(&state.db_pool, &client_order_id).await
        {
            Ok(Some(order)) => order,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "exchange_testnet_order_not_found",
                        message: "Exchange testnet order was not found.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_exchange_testnet_order",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        };

    let previous_state = parse_testnet_execution_state(&order.execution_state);
    let mut issues = Vec::new();
    let mut next_state = None;
    let mut repair_status = TestnetRepairActionStatus::Applied;

    let updated_order = match repair_request.action {
        TestnetRepairAction::MarkReconciliationRequired => {
            next_state = Some(TestnetExecutionState::ReconciliationRequired);
            match append_explicit_testnet_repair_transition(
                &state.db_pool,
                &order,
                repair_request.action,
                TestnetExecutionState::ReconciliationRequired,
                TestnetExecutionTransitionSource::OperatorMarkReconciliationRequired,
                None,
                order.exchange_order_id.as_deref(),
                repair_request
                    .reason
                    .clone()
                    .or_else(|| Some("operator_marked_reconciliation_required".to_string())),
                None,
                actor.actor_id,
                correlation_id,
                repair_request.force,
            )
            .await
            {
                Ok(updated) => updated,
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "invalid_repair_transition".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    None
                }
            }
        }
        TestnetRepairAction::ManualRecheck => {
            match state
                .exchange_testnet
                .get_order_status(&client_order_id)
                .await
            {
                Ok(status) => {
                    let payload = status.raw_payload.clone();
                    let (mapped_next_state, mapped_reason) =
                        map_rest_reconciliation_status_to_transition(&status);
                    next_state = Some(mapped_next_state);
                    match append_testnet_lifecycle_transition(
                        &state.db_pool,
                        &order,
                        mapped_next_state,
                        TestnetExecutionTransitionSource::RestReconciliation,
                        local_testnet_status_from_exchange_state(status.status),
                        status.exchange_order_id.as_deref(),
                        repair_request
                            .reason
                            .clone()
                            .or_else(|| mapped_reason.map(ToString::to_string)),
                        Some(payload),
                        actor.actor_id,
                        Some(correlation_id),
                        false,
                    )
                    .await
                    {
                        Ok(updated) => updated,
                        Err(err) => {
                            issues.push(TestnetRepairValidationIssue {
                                code: "invalid_repair_transition".to_string(),
                                message: err.to_string(),
                            });
                            repair_status = TestnetRepairActionStatus::Rejected;
                            None
                        }
                    }
                }
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "manual_recheck_failed".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    None
                }
            }
        }
        TestnetRepairAction::MarkAcked => {
            next_state = Some(TestnetExecutionState::ExchangeAcked);
            match append_explicit_testnet_repair_transition(
                &state.db_pool,
                &order,
                repair_request.action,
                TestnetExecutionState::ExchangeAcked,
                TestnetExecutionTransitionSource::RestReconciliation,
                Some("ACKED"),
                order.exchange_order_id.as_deref(),
                repair_request
                    .reason
                    .clone()
                    .or_else(|| Some("operator_marked_acked".to_string())),
                Some(json!({ "force": repair_request.force })),
                actor.actor_id,
                correlation_id,
                repair_request.force,
            )
            .await
            {
                Ok(updated) => updated,
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "invalid_repair_transition".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    None
                }
            }
        }
        TestnetRepairAction::MarkCancelled => {
            next_state = Some(TestnetExecutionState::Cancelled);
            if !repair_request.force && !order_has_cancelled_exchange_evidence(&order) {
                issues.push(TestnetRepairValidationIssue {
                    code: "cancel_evidence_required".to_string(),
                    message: "MARK_CANCELLED requires exchange evidence or force=true.".to_string(),
                });
                repair_status = TestnetRepairActionStatus::Rejected;
                None
            } else {
                match append_explicit_testnet_repair_transition(
                    &state.db_pool,
                    &order,
                    repair_request.action,
                    TestnetExecutionState::Cancelled,
                    TestnetExecutionTransitionSource::RestReconciliation,
                    Some("CANCELLED"),
                    order.exchange_order_id.as_deref(),
                    repair_request
                        .reason
                        .clone()
                        .or_else(|| Some("operator_marked_cancelled".to_string())),
                    Some(json!({ "force": repair_request.force })),
                    actor.actor_id,
                    correlation_id,
                    repair_request.force,
                )
                .await
                {
                    Ok(updated) => updated,
                    Err(err) => {
                        issues.push(TestnetRepairValidationIssue {
                            code: "invalid_repair_transition".to_string(),
                            message: err.to_string(),
                        });
                        repair_status = TestnetRepairActionStatus::Rejected;
                        None
                    }
                }
            }
        }
        TestnetRepairAction::MarkRejected => {
            next_state = Some(TestnetExecutionState::Rejected);
            match append_explicit_testnet_repair_transition(
                &state.db_pool,
                &order,
                repair_request.action,
                TestnetExecutionState::Rejected,
                TestnetExecutionTransitionSource::RestReconciliation,
                Some("REJECTED"),
                order.exchange_order_id.as_deref(),
                repair_request
                    .reason
                    .clone()
                    .or_else(|| Some("operator_marked_rejected".to_string())),
                Some(json!({ "force": repair_request.force })),
                actor.actor_id,
                correlation_id,
                repair_request.force,
            )
            .await
            {
                Ok(updated) => updated,
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "invalid_repair_transition".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    None
                }
            }
        }
        TestnetRepairAction::MarkFailed => {
            next_state = Some(TestnetExecutionState::Failed);
            match append_explicit_testnet_repair_transition(
                &state.db_pool,
                &order,
                repair_request.action,
                TestnetExecutionState::Failed,
                TestnetExecutionTransitionSource::RestReconciliation,
                Some("FAILED"),
                order.exchange_order_id.as_deref(),
                repair_request
                    .reason
                    .clone()
                    .or_else(|| Some("operator_marked_failed".to_string())),
                Some(json!({ "force": repair_request.force })),
                actor.actor_id,
                correlation_id,
                repair_request.force,
            )
            .await
            {
                Ok(updated) => updated,
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "invalid_repair_transition".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    None
                }
            }
        }
        TestnetRepairAction::SafeCancelRequest => {
            let cancel_request =
                Symbol::new(order.symbol.clone())
                    .ok()
                    .map(|symbol| ExchangeCancelRequest {
                        exchange: ExchangeName::Binance,
                        environment: ExchangeEnvironment::Testnet,
                        symbol,
                        client_order_id: client_order_id.clone(),
                        recv_window_ms: payload.recv_window_ms,
                    });
            if cancel_request.is_none() {
                issues.push(TestnetRepairValidationIssue {
                    code: "invalid_symbol".to_string(),
                    message: "Persisted exchange testnet order has an invalid symbol.".to_string(),
                });
                repair_status = TestnetRepairActionStatus::Rejected;
            }
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "exchange.testnet.repair.cancel_requested",
                    correlation_id,
                    &state.config.app_name,
                    json!({ "client_order_id": client_order_id, "action": repair_request.action.as_str() }),
                ),
            )
            .await;
            let pre_updated = match append_explicit_testnet_repair_transition(
                &state.db_pool,
                &order,
                repair_request.action,
                TestnetExecutionState::CancelRequested,
                TestnetExecutionTransitionSource::ApiCancel,
                None,
                order.exchange_order_id.as_deref(),
                repair_request
                    .reason
                    .clone()
                    .or_else(|| Some("safe_cancel_requested".to_string())),
                Some(json!({ "force": repair_request.force })),
                actor.actor_id,
                correlation_id,
                repair_request.force,
            )
            .await
            {
                Ok(Some(updated)) => updated,
                Ok(None) => order.clone(),
                Err(err) => {
                    issues.push(TestnetRepairValidationIssue {
                        code: "invalid_repair_transition".to_string(),
                        message: err.to_string(),
                    });
                    repair_status = TestnetRepairActionStatus::Rejected;
                    order.clone()
                }
            };
            next_state = Some(TestnetExecutionState::CancelRequested);
            if repair_status == TestnetRepairActionStatus::Rejected {
                None
            } else {
                match state
                    .exchange_testnet
                    .cancel_order(cancel_request.expect("cancel request should be present"))
                    .await
                {
                    Ok(ack) => {
                        let local_status = local_testnet_status_from_exchange_state(ack.status)
                            .unwrap_or(ack.status.as_str());
                        let (ack_next_state, ack_reason) = map_cancel_ack_to_transition(&ack);
                        next_state = Some(ack_next_state);
                        match append_testnet_lifecycle_transition(
                            &state.db_pool,
                            &pre_updated,
                            ack_next_state,
                            TestnetExecutionTransitionSource::ExchangeCancelAck,
                            Some(local_status),
                            ack.exchange_order_id.as_deref(),
                            repair_request
                                .reason
                                .clone()
                                .or_else(|| ack_reason.map(ToString::to_string)),
                            Some(ack.raw_payload.clone()),
                            actor.actor_id,
                            Some(correlation_id),
                            false,
                        )
                        .await
                        {
                            Ok(updated) => updated,
                            Err(err) => {
                                issues.push(TestnetRepairValidationIssue {
                                    code: "cancel_ack_transition_failed".to_string(),
                                    message: err.to_string(),
                                });
                                repair_status = TestnetRepairActionStatus::Rejected;
                                Some(pre_updated)
                            }
                        }
                    }
                    Err(err) => {
                        issues.push(TestnetRepairValidationIssue {
                            code: "safe_cancel_failed".to_string(),
                            message: err.to_string(),
                        });
                        repair_status = TestnetRepairActionStatus::Rejected;
                        Some(pre_updated)
                    }
                }
            }
        }
    };

    let repair_payload = if issues.is_empty() {
        Some(json!({ "force": repair_request.force }))
    } else {
        Some(json!({ "force": repair_request.force, "issues": issues }))
    };
    let _ = persist_testnet_repair_action(
        &state.db_pool,
        &actor,
        &client_order_id,
        &repair_request,
        repair_status,
        Some(previous_state),
        next_state,
        repair_payload,
        correlation_id,
    )
    .await;
    telemetry().inc_exchange_testnet_repair(repair_request.action.as_str(), repair_status.as_str());
    if repair_status == TestnetRepairActionStatus::Rejected {
        let reason = issues
            .first()
            .map(|value| value.code.as_str())
            .unwrap_or("rejected");
        telemetry().inc_exchange_testnet_repair_rejection(repair_request.action.as_str(), reason);
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.repair.rejected",
                correlation_id,
                &state.config.app_name,
                json!({ "client_order_id": client_order_id, "action": repair_request.action.as_str(), "reason": reason }),
            ),
        )
        .await;
    } else {
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.repair.applied",
                correlation_id,
                &state.config.app_name,
                json!({ "client_order_id": client_order_id, "action": repair_request.action.as_str(), "next_state": next_state.map(|value| value.as_str()) }),
            ),
        )
        .await;
    }

    let result = TestnetRepairResult {
        client_order_id,
        action: repair_request.action,
        status: repair_status,
        previous_state: Some(previous_state),
        next_state,
        correlation_id,
        issues,
    };
    let http_status = if repair_status == TestnetRepairActionStatus::Applied {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    let response = ExchangeTestnetRepairResponse {
        client_order_id: result.client_order_id,
        action: result.action,
        status: result.status,
        previous_state: result
            .previous_state
            .map(|value| value.as_str().to_string()),
        next_state: result.next_state.map(|value| value.as_str().to_string()),
        correlation_id: result.correlation_id,
        issues: result.issues,
        request_id: request.request_id,
        timestamp: Utc::now(),
    };
    let _ = updated_order;
    (http_status, Json(response)).into_response()
}

async fn submit_exchange_testnet_order(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<SubmitExchangeTestnetOrderRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    if !is_valid_testnet_order_confirmation(&payload.confirmation_text) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_testnet_confirmation",
                message: format!(
                    "Testnet submit requires confirmation_text exactly equal to {:?}.",
                    TESTNET_ORDER_CONFIRMATION_TEXT
                ),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let Some(risk_decision_id) = payload.risk_decision_id else {
        return exchange_testnet_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            "missing_risk_decision_id",
            "A preapproved risk_decision_id is required for testnet submission.",
            payload.symbol.clone(),
        )
        .await;
    };

    if let Err(response) = ensure_testnet_submission_allowed(
        &state,
        &actor,
        &request,
        correlation_id,
        risk_decision_id,
        &payload.symbol,
    )
    .await
    {
        return response;
    }

    let symbol = match Symbol::new(payload.symbol.clone()) {
        Ok(symbol) => symbol,
        Err(_) => {
            return exchange_testnet_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "invalid_symbol",
                "symbol must be a non-empty market symbol.",
                payload.symbol,
            )
            .await;
        }
    };
    let quantity = match payload
        .quantity
        .as_deref()
        .map(Decimal::from_str_exact)
        .transpose()
    {
        Ok(value) => value,
        Err(_) => {
            return exchange_testnet_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "invalid_quantity",
                "quantity must be a valid decimal.",
                symbol.to_string(),
            )
            .await;
        }
    };
    let quote_notional = match payload
        .quote_notional
        .as_deref()
        .map(Decimal::from_str_exact)
        .transpose()
    {
        Ok(value) => value,
        Err(_) => {
            return exchange_testnet_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "invalid_quote_notional",
                "quote_notional must be a valid decimal.",
                symbol.to_string(),
            )
            .await;
        }
    };
    let limit_price = match payload
        .limit_price
        .as_deref()
        .map(Decimal::from_str_exact)
        .transpose()
    {
        Ok(value) => value,
        Err(_) => {
            return exchange_testnet_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "invalid_limit_price",
                "limit_price must be a valid decimal.",
                symbol.to_string(),
            )
            .await;
        }
    };

    let client_order_id = generate_testnet_client_order_id(correlation_id);
    let order = ExchangeOrderRequest {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: symbol.clone(),
        side: payload.side,
        order_type: payload.order_type,
        time_in_force: payload.time_in_force,
        quantity,
        quote_notional,
        limit_price,
        client_order_id,
        recv_window_ms: payload.recv_window_ms,
        risk_decision_id: Some(risk_decision_id),
    };
    if let Err(err) = order.validate() {
        return exchange_testnet_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            "invalid_exchange_order_request",
            &err.to_string(),
            symbol.to_string(),
        )
        .await;
    }
    submit_exchange_testnet_order_request(&state, &actor, &request, correlation_id, order).await
}

async fn preview_exchange_testnet_pipeline(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ExchangeTestnetPipelinePreviewRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    match build_exchange_testnet_pipeline_preview(&state, payload.risk_decision_id, correlation_id)
        .await
    {
        Ok(prepared) => {
            let _ = insert_audit_log(
                &state.db_pool,
                correlation_id,
                &actor,
                "exchange.testnet.pipeline.previewed",
                &prepared.preview.symbol,
                &json!({
                    "risk_decision_id": prepared.preview.risk_decision_id,
                    "signal_id": prepared.preview.signal_id,
                    "strategy_id": prepared.preview.strategy_id,
                }),
            )
            .await;
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "exchange.testnet.pipeline.previewed",
                    correlation_id,
                    &state.config.app_name,
                    json!({
                        "symbol": prepared.preview.symbol,
                        "risk_decision_id": prepared.preview.risk_decision_id,
                    }),
                ),
            )
            .await;
            telemetry().inc_exchange_testnet_pipeline_run("preview_ok");
            (
                StatusCode::OK,
                Json(ExchangeTestnetPipelinePreviewResponse {
                    preview: prepared.preview,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(response) => response,
    }
}

async fn submit_exchange_testnet_pipeline(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ExchangeTestnetPipelineSubmitRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    let prepared = match build_exchange_testnet_pipeline_preview(
        &state,
        payload.risk_decision_id,
        correlation_id,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(response) => return response,
    };

    if !is_valid_testnet_pipeline_confirmation(&prepared.preview.symbol, &payload.confirmation_text)
    {
        telemetry().inc_exchange_testnet_pipeline_run("submit_confirmation_invalid");
        return exchange_testnet_pipeline_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            "invalid_testnet_pipeline_confirmation",
            &format!(
                "Testnet pipeline submit requires confirmation_text exactly equal to {:?}.",
                expected_testnet_pipeline_confirmation(&prepared.preview.symbol)
            ),
            prepared.preview.symbol.clone(),
        )
        .await;
    }

    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "exchange.testnet.pipeline.submit_requested",
        &prepared.preview.symbol,
        &json!({
            "risk_decision_id": prepared.preview.risk_decision_id,
            "signal_id": prepared.preview.signal_id,
        }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.pipeline.submit_requested",
            correlation_id,
            &state.config.app_name,
            json!({
                "symbol": prepared.preview.symbol,
                "risk_decision_id": prepared.preview.risk_decision_id,
            }),
        ),
    )
    .await;
    telemetry().inc_exchange_testnet_pipeline_run("submit_attempt");

    let response = submit_exchange_testnet_order_request(
        &state,
        &actor,
        &request,
        correlation_id,
        prepared.order,
    )
    .await;

    if response.status().is_success() {
        let client_order_id = generate_testnet_client_order_id(correlation_id);
        match get_exchange_testnet_order_by_client_order_id(&state.db_pool, &client_order_id).await
        {
            Ok(Some(order_record)) => {
                match build_exchange_testnet_order_view(&state.db_pool, order_record).await {
                    Ok(order) => {
                        telemetry().inc_exchange_testnet_pipeline_run("submit_ok");
                        (
                            StatusCode::CREATED,
                            Json(ExchangeTestnetPipelineSubmitResponse {
                                preview: prepared.preview,
                                order,
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response()
                    }
                    Err(_) => response,
                }
            }
            _ => response,
        }
    } else {
        telemetry().inc_exchange_testnet_pipeline_run("submit_rejected");
        response
    }
}

async fn run_exchange_testnet_shadow_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<TestnetShadowRunRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let shadow_state = shadow_runtime_state(&state);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }

    match run_testnet_shadow_once(&shadow_state, Some(&actor), payload).await {
        Ok(run) => (
            StatusCode::OK,
            Json(TestnetShadowRunResponse {
                run,
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
                "failed to run testnet shadow"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_run_testnet_shadow",
                    message: "Testnet shadow run could not be completed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn list_exchange_testnet_shadow_runs_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Query(query): Query<TestnetShadowRunsQuery>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_EXCHANGE_TESTNET_LIMIT)
        .clamp(1, MAX_EXCHANGE_TESTNET_LIMIT);

    match list_testnet_shadow_runs(&state.db_pool, limit).await {
        Ok(records) => match records
            .iter()
            .map(db::testnet_shadow_run_result_from_record)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(runs) => (
                StatusCode::OK,
                Json(TestnetShadowRunsResponse {
                    runs,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_testnet_shadow_runs",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list testnet shadow runs"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_testnet_shadow_runs",
                    message: "Testnet shadow runs could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_exchange_testnet_shadow_run_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(run_id): Path<Uuid>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_testnet_shadow_run_by_id(&state.db_pool, run_id).await {
        Ok(Some(record)) => match db::testnet_shadow_run_result_from_record(&record) {
            Ok(run) => (
                StatusCode::OK,
                Json(TestnetShadowRunResponse {
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
                    error: "failed_to_map_testnet_shadow_run",
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
                error: "testnet_shadow_run_not_found",
                message: "Testnet shadow run was not found.".to_string(),
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
                "failed to query testnet shadow run"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_testnet_shadow_run",
                    message: "Testnet shadow run could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

fn testnet_shadow_promotion_ttl() -> Duration {
    env::var("TESTNET_SHADOW_PROMOTION_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_TESTNET_SHADOW_PROMOTION_TTL_SECONDS))
}

async fn ensure_shadow_promotion_price_fresh(
    state: &AppState,
    request: &RequestContext,
    _correlation_id: Uuid,
    symbol_text: &str,
) -> std::result::Result<(), Response> {
    let symbol = Symbol::new(symbol_text.to_string()).map_err(|_| {
        (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "invalid_symbol",
                message: "Persisted shadow run symbol is invalid.".to_string(),
                request_id: request.request_id.clone(),
                correlation_id: request.correlation_id.clone(),
                timestamp: Utc::now(),
            }),
        )
            .into_response()
    })?;

    let latest_tick = get_latest_market_tick(&state.db_pool, state.market_config.exchange, &symbol)
        .await
        .map_err(|err| {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                symbol = %symbol_text,
                "failed to load latest tick for shadow promotion"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_market_tick",
                    message: "Latest market tick could not be loaded.".to_string(),
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        })?;

    if let Some(tick) = latest_tick.as_ref() {
        if tick.price > Decimal::ZERO
            && !is_testnet_pipeline_price_stale(
                tick.received_at,
                Utc::now(),
                state.market_config.stale_threshold,
            )
        {
            return Ok(());
        }
    }

    let latest_candle = list_candles(
        &state.db_pool,
        state.market_config.exchange,
        &symbol,
        CandleInterval::OneMinute,
        1,
    )
    .await
    .map_err(|err| {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            symbol = %symbol_text,
            "failed to load fallback candle for shadow promotion"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_market_candle",
                message: "Latest fallback candle could not be loaded.".to_string(),
                request_id: request.request_id.clone(),
                correlation_id: request.correlation_id.clone(),
                timestamp: Utc::now(),
            }),
        )
            .into_response()
    })?
    .into_iter()
    .find(|candle| candle.is_closed);

    if let Some(candle) = latest_candle {
        if candle.close > Decimal::ZERO
            && !is_testnet_pipeline_price_stale(
                candle.close_time,
                Utc::now(),
                state.market_config.stale_threshold,
            )
        {
            return Ok(());
        }
    }

    Err((
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error: "stale_price",
            message: "A fresh local testnet reference price is required before shadow promotion."
                .to_string(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            timestamp: Utc::now(),
        }),
    )
        .into_response())
}

async fn exchange_testnet_shadow_promotion_rejected_response(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    shadow_run_id: Option<Uuid>,
    symbol: Option<&str>,
    error_code: &'static str,
    message: &str,
) -> Response {
    let target = shadow_run_id
        .map(|value| value.to_string())
        .or_else(|| symbol.map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string());
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        actor,
        "exchange.testnet.shadow_promotion.rejected",
        &target,
        &json!({
            "shadow_run_id": shadow_run_id,
            "symbol": symbol,
            "error": error_code,
            "message": message,
        }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.shadow_promotion.rejected",
            correlation_id,
            &state.config.app_name,
            json!({
                "shadow_run_id": shadow_run_id,
                "symbol": symbol,
                "error": error_code,
                "message": message,
            }),
        ),
    )
    .await;
    telemetry().inc_exchange_testnet_shadow_promotion("rejected");

    (
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error: error_code,
            message: message.to_string(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn preview_exchange_testnet_shadow_promotion_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<TestnetShadowPromotionRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    if state.exchange_testnet_environment != ExchangeEnvironment::Testnet {
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(payload.shadow_run_id),
            None,
            "invalid_exchange_environment",
            "Only testnet environment is allowed.",
        )
        .await;
    }

    let shadow_run = match get_testnet_shadow_run_by_id(&state.db_pool, payload.shadow_run_id).await
    {
        Ok(Some(record)) => record,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "testnet_shadow_run_not_found",
                    message: "Testnet shadow run was not found.".to_string(),
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
                "failed to load testnet shadow run for promotion preview"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_testnet_shadow_run",
                    message: "Testnet shadow run could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    if let Ok(Some(existing)) =
        get_active_testnet_shadow_promotion_for_shadow_run(&state.db_pool, shadow_run.id).await
    {
        let code = if existing.status == TestnetShadowPromotionStatus::Submitted.as_str() {
            "already_promoted"
        } else {
            "promotion_already_previewed"
        };
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(shadow_run.id),
            Some(&shadow_run.symbol),
            code,
            "A non-terminal shadow promotion already exists for this shadow run.",
        )
        .await;
    }

    if shadow_run.decision != "WOULD_SUBMIT" {
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(shadow_run.id),
            Some(&shadow_run.symbol),
            "shadow_decision_not_would_submit",
            "Only persisted WOULD_SUBMIT shadow runs can be promoted.",
        )
        .await;
    }

    let risk_decision_id = match shadow_run.risk_decision_id {
        Some(value) => value,
        None => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "missing_risk_decision_id",
                "Shadow run is missing a persisted risk_decision_id.",
            )
            .await;
        }
    };

    let would_submit_payload = match shadow_run.would_submit_payload.clone() {
        Some(value) => value,
        None => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "missing_would_submit_payload",
                "Shadow run is missing a persisted would-submit payload.",
            )
            .await;
        }
    };

    let would_submit_payload: aegis_core::TestnetShadowIntent =
        match serde_json::from_value(would_submit_payload) {
            Ok(value) => value,
            Err(err) => {
                return exchange_testnet_shadow_promotion_rejected_response(
                    &state,
                    &actor,
                    &request,
                    correlation_id,
                    Some(shadow_run.id),
                    Some(&shadow_run.symbol),
                    "invalid_would_submit_payload",
                    &format!("Persisted would-submit payload is invalid: {err}"),
                )
                .await;
            }
        };

    match get_system_state(&state.db_pool).await {
        Ok(system_state) if system_state.kill_switch_enabled => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "kill_switch_active",
                "Global kill switch is active.",
            )
            .await;
        }
        Ok(_) => {}
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load system state for shadow promotion preview"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_read_system_state",
                    message: "System state could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    }

    match get_risk_decision_by_id(&state.db_pool, risk_decision_id).await {
        Ok(Some(record)) if record.decision == "APPROVED" => {}
        Ok(Some(_)) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "risk_decision_not_approved",
                "risk_decision_id must still reference an APPROVED risk decision.",
            )
            .await;
        }
        Ok(None) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "risk_decision_not_found",
                "risk_decision_id must reference an existing persisted risk decision.",
            )
            .await;
        }
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load risk decision for shadow promotion preview"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decision",
                    message: "Risk decision could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    }

    let strategy_id = match shadow_run.strategy_id.parse::<StrategyId>() {
        Ok(value) => value,
        Err(err) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "invalid_strategy_id",
                &err.to_string(),
            )
            .await;
        }
    };

    match ensure_strategy_config(&state, strategy_id).await {
        Ok(config) if config.enabled => {}
        Ok(_) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "strategy_disabled",
                "Strategy config is disabled and cannot be promoted to testnet.",
            )
            .await;
        }
        Err(err) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(shadow_run.id),
                Some(&shadow_run.symbol),
                "failed_to_load_strategy_config",
                &err.to_string(),
            )
            .await;
        }
    }

    if let Err(response) =
        ensure_shadow_promotion_price_fresh(&state, &request, correlation_id, &shadow_run.symbol)
            .await
    {
        return response;
    }

    let expires_at = Utc::now()
        + chrono::Duration::from_std(testnet_shadow_promotion_ttl())
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
    let record = TestnetShadowPromotionRecord {
        id: Uuid::new_v4(),
        shadow_run_id: shadow_run.id,
        status: TestnetShadowPromotionStatus::Previewed.as_str().to_string(),
        strategy_id: Some(shadow_run.strategy_id.clone()),
        symbol: Some(shadow_run.symbol.clone()),
        timeframe: Some(shadow_run.timeframe.clone()),
        signal_id: shadow_run.signal_id,
        risk_decision_id: Some(risk_decision_id),
        would_submit_payload: serde_json::to_value(&would_submit_payload).unwrap_or(Value::Null),
        resolved_price: shadow_run.resolved_price,
        price_source: shadow_run.price_source.clone(),
        rejection_reasons: Vec::new(),
        testnet_order_id: None,
        client_order_id: None,
        expires_at,
        created_by: actor.actor_id,
        submitted_by: None,
        created_at: Utc::now(),
        submitted_at: None,
        correlation_id: Some(correlation_id),
    };

    let persisted = match insert_testnet_shadow_promotion(&state.db_pool, &record).await {
        Ok(value) => value,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to persist shadow promotion preview"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_persist_shadow_promotion",
                    message: "Shadow promotion preview could not be persisted.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let preview = match db::testnet_shadow_promotion_from_record(&persisted) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_shadow_promotion",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "exchange.testnet.shadow_promotion.previewed",
        &shadow_run.id.to_string(),
        &json!({
            "promotion_id": preview.promotion_id,
            "shadow_run_id": preview.shadow_run_id,
            "risk_decision_id": preview.risk_decision_id,
        }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.shadow_promotion.previewed",
            correlation_id,
            &state.config.app_name,
            json!({
                "promotion_id": preview.promotion_id,
                "shadow_run_id": preview.shadow_run_id,
                "symbol": preview.symbol,
            }),
        ),
    )
    .await;
    telemetry().inc_exchange_testnet_shadow_promotion("previewed");

    (
        StatusCode::OK,
        Json(TestnetShadowPromotionResponse {
            promotion: preview,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn list_exchange_testnet_shadow_promotions_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Query(query): Query<TestnetShadowPromotionsQuery>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_EXCHANGE_TESTNET_LIMIT)
        .clamp(1, MAX_EXCHANGE_TESTNET_LIMIT);

    match list_testnet_shadow_promotions(&state.db_pool, limit).await {
        Ok(records) => match records
            .iter()
            .map(db::testnet_shadow_promotion_from_record)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(promotions) => (
                StatusCode::OK,
                Json(TestnetShadowPromotionsResponse {
                    promotions,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_shadow_promotions",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list testnet shadow promotions"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_shadow_promotions",
                    message: "Shadow promotions could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_exchange_testnet_shadow_promotion_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(promotion_id): Path<Uuid>,
) -> impl IntoResponse {
    let request = request_context(request);
    match get_testnet_shadow_promotion_by_id(&state.db_pool, promotion_id).await {
        Ok(Some(record)) => match db::testnet_shadow_promotion_from_record(&record) {
            Ok(promotion) => (
                StatusCode::OK,
                Json(TestnetShadowPromotionResponse {
                    promotion,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_shadow_promotion",
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
                error: "testnet_shadow_promotion_not_found",
                message: "Testnet shadow promotion was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_shadow_promotion",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn submit_exchange_testnet_shadow_promotion_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Path(promotion_id): Path<Uuid>,
    Json(payload): Json<TestnetShadowPromotionSubmitRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    let promotion = match get_testnet_shadow_promotion_by_id(&state.db_pool, promotion_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "testnet_shadow_promotion_not_found",
                    message: "Testnet shadow promotion was not found.".to_string(),
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
                "failed to load shadow promotion before submit"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_shadow_promotion",
                    message: "Shadow promotion could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let symbol = promotion.symbol.clone().unwrap_or_default();
    if !is_valid_testnet_shadow_promotion_confirmation(&symbol, &payload.confirmation_text) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_confirmation_text",
                message: format!(
                    "Shadow promotion submit requires confirmation_text exactly equal to {:?}.",
                    expected_testnet_shadow_promotion_confirmation(&symbol)
                ),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    if promotion.status != TestnetShadowPromotionStatus::Previewed.as_str() {
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(promotion.shadow_run_id),
            Some(&symbol),
            "duplicate_submit",
            "Only PREVIEWED promotions can be submitted.",
        )
        .await;
    }

    if promotion.expires_at <= Utc::now() {
        let reasons = vec!["promotion_expired".to_string()];
        let _ = update_testnet_shadow_promotion_submission(
            &state.db_pool,
            promotion.id,
            TestnetShadowPromotionStatus::Expired.as_str(),
            &reasons,
            None,
            None,
            actor.actor_id,
            Some(Utc::now()),
        )
        .await;
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(promotion.shadow_run_id),
            Some(&symbol),
            "promotion_expired",
            "Shadow promotion has expired and must be previewed again.",
        )
        .await;
    }

    match get_system_state(&state.db_pool).await {
        Ok(system_state) if system_state.kill_switch_enabled => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(promotion.shadow_run_id),
                Some(&symbol),
                "kill_switch_active",
                "Global kill switch is active.",
            )
            .await;
        }
        Ok(_) => {}
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load system state for shadow promotion submit"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_read_system_state",
                    message: "System state could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    }

    let risk_decision_id = match promotion.risk_decision_id {
        Some(value) => value,
        None => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(promotion.shadow_run_id),
                Some(&symbol),
                "missing_risk_decision_id",
                "Promotion is missing risk_decision_id.",
            )
            .await;
        }
    };

    match get_risk_decision_by_id(&state.db_pool, risk_decision_id).await {
        Ok(Some(record)) if record.decision == "APPROVED" => {}
        Ok(Some(_)) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(promotion.shadow_run_id),
                Some(&symbol),
                "risk_decision_not_approved",
                "risk_decision_id must still reference an APPROVED risk decision.",
            )
            .await;
        }
        Ok(None) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(promotion.shadow_run_id),
                Some(&symbol),
                "risk_decision_not_found",
                "risk_decision_id must reference an existing persisted risk decision.",
            )
            .await;
        }
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load risk decision for shadow promotion submit"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decision",
                    message: "Risk decision could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    }

    let would_submit_payload: aegis_core::TestnetShadowIntent =
        match serde_json::from_value(promotion.would_submit_payload.clone()) {
            Ok(value) => value,
            Err(err) => {
                return exchange_testnet_shadow_promotion_rejected_response(
                    &state,
                    &actor,
                    &request,
                    correlation_id,
                    Some(promotion.shadow_run_id),
                    Some(&symbol),
                    "invalid_would_submit_payload",
                    &format!("Persisted would-submit payload is invalid: {err}"),
                )
                .await;
            }
        };

    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "exchange.testnet.shadow_promotion.submit_requested",
        &promotion.id.to_string(),
        &json!({
            "promotion_id": promotion.id,
            "shadow_run_id": promotion.shadow_run_id,
            "symbol": symbol,
        }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.shadow_promotion.submit_requested",
            correlation_id,
            &state.config.app_name,
            json!({
                "promotion_id": promotion.id,
                "shadow_run_id": promotion.shadow_run_id,
                "symbol": symbol,
            }),
        ),
    )
    .await;

    let symbol_model = match Symbol::new(would_submit_payload.symbol.to_string()) {
        Ok(value) => value,
        Err(_) => {
            return exchange_testnet_shadow_promotion_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                Some(promotion.shadow_run_id),
                Some(&symbol),
                "invalid_symbol",
                "Persisted would-submit payload contains an invalid symbol.",
            )
            .await;
        }
    };
    let client_order_id = generate_testnet_client_order_id(correlation_id);
    let order = ExchangeOrderRequest {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: symbol_model,
        side: would_submit_payload.side,
        order_type: would_submit_payload.order_type,
        time_in_force: would_submit_payload.time_in_force,
        quantity: would_submit_payload.quantity,
        quote_notional: would_submit_payload.quote_notional,
        limit_price: would_submit_payload.limit_price,
        client_order_id: client_order_id.clone(),
        recv_window_ms: None,
        risk_decision_id: Some(risk_decision_id),
    };
    if let Err(err) = order.validate() {
        return exchange_testnet_shadow_promotion_rejected_response(
            &state,
            &actor,
            &request,
            correlation_id,
            Some(promotion.shadow_run_id),
            Some(&symbol),
            "invalid_exchange_order_request",
            &err.to_string(),
        )
        .await;
    }

    let persisted_order = ExchangeTestnetOrderRecord {
        id: Uuid::new_v4(),
        exchange: ExchangeName::Binance.as_str().to_string(),
        environment: ExchangeEnvironment::Testnet.as_str().to_string(),
        client_order_id: client_order_id.clone(),
        exchange_order_id: None,
        symbol: order.symbol.to_string(),
        side: order.side.as_str().to_string(),
        order_type: order.order_type.as_str().to_string(),
        time_in_force: order.time_in_force.map(|value| value.as_str().to_string()),
        requested_qty: order.quantity,
        requested_notional: order.quote_notional,
        limit_price: order.limit_price,
        status: "SUBMIT_REQUESTED".to_string(),
        execution_state: TestnetExecutionState::OrderSubmitRequested
            .as_str()
            .to_string(),
        ack_payload: None,
        latest_status_payload: None,
        risk_decision_id: order.risk_decision_id,
        created_by: actor.actor_id,
        last_transition_at: Some(Utc::now()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    if let Err(err) = insert_exchange_testnet_order(&state.db_pool, &persisted_order).await {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            "failed to persist exchange testnet order for shadow promotion"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_persist_exchange_testnet_order",
                message: "Exchange testnet order could not be persisted.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let _ = append_exchange_testnet_lifecycle_event_and_update_order(
        &state.db_pool,
        &ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(persisted_order.id),
            client_order_id: persisted_order.client_order_id.clone(),
            previous_state: None,
            next_state: TestnetExecutionState::OrderSubmitRequested
                .as_str()
                .to_string(),
            transition_source: TestnetExecutionTransitionSource::ApiSubmit
                .as_str()
                .to_string(),
            reason: Some("submit_requested".to_string()),
            payload: None,
            created_by: actor.actor_id,
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        },
        None,
        Some("SUBMIT_REQUESTED"),
        TestnetExecutionState::OrderSubmitRequested,
        None,
        None,
    )
    .await;

    telemetry().inc_exchange_testnet_shadow_promotion_submit("attempt");
    telemetry().inc_exchange_testnet_request("submit_order", "attempt");
    match state.exchange_testnet.submit_order(order).await {
        Ok(ack) => {
            let local_status =
                local_testnet_status_from_exchange_state(ack.status).unwrap_or(ack.status.as_str());
            let (next_state, reason) = map_exchange_ack_to_transition(&ack);
            let ack_payload = ack.raw_payload.clone();
            let _ = append_testnet_lifecycle_transition(
                &state.db_pool,
                &persisted_order,
                next_state,
                TestnetExecutionTransitionSource::ExchangeAck,
                Some(local_status),
                ack.exchange_order_id.as_deref(),
                reason.map(ToString::to_string),
                Some(ack_payload),
                actor.actor_id,
                Some(correlation_id),
                true,
            )
            .await;
            let _ = update_testnet_shadow_promotion_submission(
                &state.db_pool,
                promotion.id,
                TestnetShadowPromotionStatus::Submitted.as_str(),
                &Vec::new(),
                Some(persisted_order.id),
                Some(&persisted_order.client_order_id),
                actor.actor_id,
                Some(Utc::now()),
            )
            .await;
            let _ = insert_audit_log(
                &state.db_pool,
                correlation_id,
                &actor,
                "exchange.testnet.shadow_promotion.submitted",
                &promotion.id.to_string(),
                &json!({
                    "shadow_run_id": promotion.shadow_run_id,
                    "testnet_order_id": persisted_order.id,
                    "client_order_id": persisted_order.client_order_id,
                }),
            )
            .await;
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "exchange.testnet.shadow_promotion.submitted",
                    correlation_id,
                    &state.config.app_name,
                    json!({
                        "promotion_id": promotion.id,
                        "shadow_run_id": promotion.shadow_run_id,
                        "client_order_id": persisted_order.client_order_id,
                    }),
                ),
            )
            .await;
            telemetry().inc_exchange_testnet_shadow_promotion("submitted");
            telemetry().inc_exchange_testnet_shadow_promotion_submit("ok");
            (
                StatusCode::CREATED,
                Json(TestnetShadowPromotionSubmitResponse {
                    result: TestnetShadowPromotionResult {
                        promotion_id: promotion.id,
                        shadow_run_id: promotion.shadow_run_id,
                        testnet_order_id: persisted_order.id,
                        client_order_id: persisted_order.client_order_id,
                        execution_state: next_state,
                        correlation_id,
                    },
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            let reasons = vec!["submit_failed".to_string()];
            let _ = update_testnet_shadow_promotion_submission(
                &state.db_pool,
                promotion.id,
                TestnetShadowPromotionStatus::Rejected.as_str(),
                &reasons,
                Some(persisted_order.id),
                Some(&persisted_order.client_order_id),
                actor.actor_id,
                Some(Utc::now()),
            )
            .await;
            telemetry().inc_exchange_testnet_shadow_promotion_submit("error");
            exchange_testnet_adapter_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "submit_order",
                err,
                symbol,
            )
            .await
        }
    }
}

async fn get_exchange_testnet_shadow_runner_status_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let shadow_state = shadow_runtime_state(&state);

    match load_testnet_shadow_runner_snapshot(&shadow_state).await {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(TestnetShadowRunnerStatusResponse {
                config: snapshot.config,
                state: snapshot.state,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_testnet_shadow_runner_status",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_exchange_testnet_shadow_runner_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let shadow_state = shadow_runtime_state(&state);

    match load_testnet_shadow_runner_snapshot(&shadow_state).await {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(TestnetShadowRunnerConfigResponse {
                config: snapshot.config,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_testnet_shadow_runner_config",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn validate_exchange_testnet_shadow_runner_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<TestnetShadowRunnerConfigInput>,
) -> impl IntoResponse {
    let request = request_context(request);
    let validation = validate_testnet_shadow_runner_config(&payload);
    let event_type = if validation.valid {
        "exchange.testnet.shadow_runner.config.validated"
    } else {
        "exchange.testnet.shadow_runner.config.rejected"
    };
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            parse_correlation_id(&request.correlation_id),
            state.config.app_name.clone(),
            json!({ "issues": validation.issues }),
        ),
    )
    .await;

    (
        if validation.valid {
            StatusCode::OK
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        },
        Json(TestnetShadowRunnerConfigValidationResponse {
            validation,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn update_exchange_testnet_shadow_runner_config_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<TestnetShadowRunnerConfigInput>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    let state_actor = actor
        .as_ref()
        .map(state_actor_from_authenticated)
        .unwrap_or_else(|| StateActor::system("anonymous"));
    let actor_id = actor.as_ref().map(|value| value.user_id);
    let shadow_state = shadow_runtime_state(&state);
    let validation = validate_testnet_shadow_runner_config(&payload);

    if !validation.valid {
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.shadow_runner.config.rejected",
                parse_correlation_id(&request.correlation_id),
                state.config.app_name.clone(),
                json!({ "issues": validation.issues, "actor_id": actor_id }),
            ),
        )
        .await;
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(TestnetShadowRunnerConfigValidationResponse {
                validation,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    match persist_testnet_shadow_runner_config(
        &shadow_state,
        &payload,
        actor.as_ref().map(|value| value.user_id),
    )
    .await
    {
        Ok(config) => {
            let correlation_id = parse_correlation_id(&request.correlation_id);
            let _ = insert_audit_log(
                &state.db_pool,
                correlation_id,
                &state_actor,
                "exchange.testnet.shadow_runner.config.updated",
                "testnet_shadow_runner_config",
                &json!({ "config": config, "actor_id": actor_id }),
            )
            .await;
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "exchange.testnet.shadow_runner.config.updated",
                    correlation_id,
                    state.config.app_name.clone(),
                    json!({ "config": config, "actor_id": actor_id }),
                ),
            )
            .await;
            (
                StatusCode::OK,
                Json(TestnetShadowRunnerConfigResponse {
                    config,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_update_testnet_shadow_runner_config",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn control_exchange_testnet_shadow_runner_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<TestnetShadowRunnerControlRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    let Some(ref authenticated_actor) = actor else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "unauthorized",
                message: "Authentication is required.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    };
    if matches!(
        payload.action,
        TestnetShadowRunnerControlAction::Start | TestnetShadowRunnerControlAction::Stop
    ) && authenticated_actor.role != UserRole::Owner
    {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "forbidden",
                message: "Only OWNER may start or stop the shadow runner.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let shadow_state = shadow_runtime_state(&state);
    let state_actor = state_actor_from_authenticated(authenticated_actor);
    let correlation_id = payload
        .correlation_id
        .take()
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    match apply_testnet_shadow_runner_control_action(
        &shadow_state,
        Some(&state_actor),
        payload.action,
        correlation_id,
    )
    .await
    {
        Ok((runner_state, tick)) => {
            let _ = insert_audit_log(
                &state.db_pool,
                correlation_id,
                &state_actor,
                "exchange.testnet.shadow_runner.control",
                "testnet_shadow_runner_state",
                &json!({
                    "action": payload.action.as_str(),
                    "state": runner_state,
                    "tick": tick,
                }),
            )
            .await;
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "exchange.testnet.shadow_runner.controlled",
                    correlation_id,
                    state.config.app_name.clone(),
                    json!({
                        "action": payload.action.as_str(),
                        "state": runner_state,
                        "tick": tick,
                        "actor_id": authenticated_actor.user_id,
                    }),
                ),
            )
            .await;
            (
                StatusCode::OK,
                Json(TestnetShadowRunnerControlResponse {
                    state: runner_state,
                    tick,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "failed_to_control_testnet_shadow_runner",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn cancel_exchange_testnet_order(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Path(client_order_id): Path<String>,
    Json(payload): Json<CancelExchangeTestnetOrderRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    if !is_valid_testnet_order_confirmation(&payload.confirmation_text) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_testnet_confirmation",
                message: format!(
                    "Testnet cancel requires confirmation_text exactly equal to {:?}.",
                    TESTNET_ORDER_CONFIRMATION_TEXT
                ),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let existing_order =
        match get_exchange_testnet_order_by_client_order_id(&state.db_pool, &client_order_id).await
        {
            Ok(Some(order)) => order,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "exchange_testnet_order_not_found",
                        message: "Exchange testnet order was not found.".to_string(),
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
                    "failed to load exchange testnet order before cancel"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_exchange_testnet_order",
                        message: "Exchange testnet order could not be loaded.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        };

    let request_model = match Symbol::new(existing_order.symbol.clone()) {
        Ok(symbol) => ExchangeCancelRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol,
            client_order_id: client_order_id.clone(),
            recv_window_ms: payload.recv_window_ms,
        },
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "invalid_persisted_exchange_testnet_order",
                    message: "Persisted exchange testnet order has an invalid symbol.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    };

    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        &actor,
        "exchange.testnet.order.cancel_requested",
        &client_order_id,
        &json!({ "symbol": existing_order.symbol }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.order.cancel_requested",
            correlation_id,
            &state.config.app_name,
            json!({ "client_order_id": client_order_id }),
        ),
    )
    .await;

    let existing_order = match append_testnet_lifecycle_transition(
        &state.db_pool,
        &existing_order,
        TestnetExecutionState::CancelRequested,
        TestnetExecutionTransitionSource::ApiCancel,
        None,
        existing_order.exchange_order_id.as_deref(),
        Some("cancel_requested".to_string()),
        None,
        actor.actor_id,
        Some(correlation_id),
        false,
    )
    .await
    {
        Ok(Some(updated)) => updated,
        _ => existing_order,
    };

    telemetry().inc_exchange_testnet_request("cancel_order", "attempt");
    match state.exchange_testnet.cancel_order(request_model).await {
        Ok(ack) => {
            cancel_exchange_testnet_order_success(
                &state,
                &actor,
                &request,
                correlation_id,
                existing_order,
                ack,
            )
            .await
        }
        Err(err) => {
            exchange_testnet_adapter_rejected_response(
                &state,
                &actor,
                &request,
                correlation_id,
                "cancel_order",
                err,
                client_order_id,
            )
            .await
        }
    }
}

async fn reconcile_exchange_testnet_orders_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<ReconcileExchangeTestnetOrdersRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let limit = bounded_exchange_testnet_limit(payload.limit);
    let request_model = ExchangeReconciliationRequest {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        limit,
        status_filter: payload.status_filter.unwrap_or_else(|| {
            vec![
                "ACKED".to_string(),
                "NEW".to_string(),
                "PARTIALLY_FILLED".to_string(),
            ]
        }),
        correlation_id: payload
            .correlation_id
            .or_else(|| Some(parse_correlation_id(&request.correlation_id))),
    };

    match reconcile_testnet_orders(
        &state.db_pool,
        state.exchange_testnet.as_ref(),
        &state.config.app_name,
        &actor,
        &request_model,
    )
    .await
    {
        Ok(details) => (
            StatusCode::OK,
            Json(ExchangeReconciliationResultResponse {
                result: run_result_from_run(&details.run),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(ReconcileTestnetOrdersError::Validation(err)) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_exchange_reconciliation_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(ReconcileTestnetOrdersError::Failed {
            run_id,
            correlation_id,
            reason,
        }) => (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse {
                error: "exchange_reconciliation_failed",
                message: format!(
                    "Exchange reconciliation failed for run {run_id} (correlation_id {correlation_id}): {reason}"
                ),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(ReconcileTestnetOrdersError::Unexpected(err)) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to reconcile exchange testnet orders"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_reconcile_exchange_testnet_orders",
                    message: "Exchange testnet reconciliation could not be completed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn list_exchange_reconciliation_runs_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Query(query): Query<ExchangeReconciliationRunsQuery>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = bounded_exchange_reconciliation_runs_limit(query.limit);

    match list_exchange_reconciliation_runs(
        &state.db_pool,
        ExchangeEnvironment::Testnet.as_str(),
        limit,
    )
    .await
    {
        Ok(runs) => match runs
            .iter()
            .map(run_from_record)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(runs) => (
                StatusCode::OK,
                Json(ExchangeReconciliationRunsResponse {
                    runs,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_exchange_reconciliation_runs",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list exchange reconciliation runs"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_exchange_reconciliation_runs",
                    message: "Exchange reconciliation runs could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_exchange_reconciliation_run_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(run_id): Path<Uuid>,
) -> impl IntoResponse {
    let request = request_context(request);

    match db::get_exchange_reconciliation_run(&state.db_pool, run_id).await {
        Ok(Some(run)) => match run_from_record(&run) {
            Ok(run) => (
                StatusCode::OK,
                Json(ExchangeReconciliationRunResponse {
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
                    error: "failed_to_map_exchange_reconciliation_run",
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
                error: "exchange_reconciliation_run_not_found",
                message: "Exchange reconciliation run was not found.".to_string(),
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
                "failed to query exchange reconciliation run"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_exchange_reconciliation_run",
                    message: "Exchange reconciliation run could not be loaded.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn list_exchange_reconciliation_mismatches_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Path(run_id): Path<Uuid>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_exchange_reconciliation_mismatches(&state.db_pool, run_id).await {
        Ok(mismatches) => match mismatches
            .iter()
            .map(mismatch_from_record)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(mismatches) => (
                StatusCode::OK,
                Json(ExchangeReconciliationMismatchesResponse {
                    mismatches,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_exchange_reconciliation_mismatches",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list exchange reconciliation mismatches"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_exchange_reconciliation_mismatches",
                    message: "Exchange reconciliation mismatches could not be listed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn submit_exchange_testnet_order_success(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    fallback_record: ExchangeTestnetOrderRecord,
    ack: ExchangeOrderAck,
) -> Response {
    telemetry().inc_exchange_testnet_request("submit_order", "ok");
    let local_status =
        local_testnet_status_from_exchange_state(ack.status).unwrap_or(ack.status.as_str());
    let (next_state, reason) = map_exchange_ack_to_transition(&ack);
    telemetry().inc_exchange_testnet_order(
        &fallback_record.symbol,
        &fallback_record.side,
        local_status,
    );
    let ack_payload = ack.raw_payload.clone();
    let updated = append_testnet_lifecycle_transition(
        &state.db_pool,
        &fallback_record,
        next_state,
        TestnetExecutionTransitionSource::ExchangeAck,
        Some(local_status),
        ack.exchange_order_id.as_deref(),
        reason.map(ToString::to_string),
        Some(ack_payload),
        actor.actor_id,
        Some(correlation_id),
        true,
    )
    .await;
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        actor,
        "exchange.testnet.order.acked",
        &fallback_record.client_order_id,
        &json!({ "status": local_status, "exchange_order_id": ack.exchange_order_id }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.order.acked",
            correlation_id,
            &state.config.app_name,
            json!({ "client_order_id": fallback_record.client_order_id, "status": local_status }),
        ),
    )
    .await;

    match updated {
        Ok(Some(order_record)) => {
            let view = build_exchange_testnet_order_view(&state.db_pool, order_record)
                .await
                .unwrap_or_else(|_| exchange_testnet_order_view(fallback_record.clone()));
            (
                StatusCode::CREATED,
                Json(ExchangeTestnetOrderResponse {
                    order: view,
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Ok(None) | Err(_) => {
            let view = build_exchange_testnet_order_view(&state.db_pool, fallback_record.clone())
                .await
                .unwrap_or_else(|_| exchange_testnet_order_view(fallback_record));
            (
                StatusCode::CREATED,
                Json(ExchangeTestnetOrderResponse {
                    order: view,
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn submit_exchange_testnet_order_request(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    order: ExchangeOrderRequest,
) -> Response {
    let client_order_id = generate_testnet_client_order_id(correlation_id);
    let persisted = ExchangeTestnetOrderRecord {
        id: Uuid::new_v4(),
        exchange: ExchangeName::Binance.as_str().to_string(),
        environment: ExchangeEnvironment::Testnet.as_str().to_string(),
        client_order_id: client_order_id.clone(),
        exchange_order_id: None,
        symbol: order.symbol.to_string(),
        side: order.side.as_str().to_string(),
        order_type: order.order_type.as_str().to_string(),
        time_in_force: order.time_in_force.map(|value| value.as_str().to_string()),
        requested_qty: order.quantity,
        requested_notional: order.quote_notional,
        limit_price: order.limit_price,
        status: "SUBMIT_REQUESTED".to_string(),
        execution_state: TestnetExecutionState::OrderSubmitRequested
            .as_str()
            .to_string(),
        ack_payload: None,
        latest_status_payload: None,
        risk_decision_id: order.risk_decision_id,
        created_by: actor.actor_id,
        last_transition_at: Some(Utc::now()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        actor,
        "exchange.testnet.order.submit_requested",
        &client_order_id,
        &json!({ "symbol": persisted.symbol, "risk_decision_id": order.risk_decision_id }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.order.submit_requested",
            correlation_id,
            &state.config.app_name,
            json!({ "client_order_id": client_order_id, "symbol": persisted.symbol }),
        ),
    )
    .await;

    if let Err(err) = insert_exchange_testnet_order(&state.db_pool, &persisted).await {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            "failed to persist exchange testnet order request"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_persist_exchange_testnet_order",
                message: "Exchange testnet order could not be persisted.".to_string(),
                request_id: request.request_id.clone(),
                correlation_id: request.correlation_id.clone(),
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let _ = append_exchange_testnet_lifecycle_event_and_update_order(
        &state.db_pool,
        &ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(persisted.id),
            client_order_id: persisted.client_order_id.clone(),
            previous_state: None,
            next_state: TestnetExecutionState::OrderSubmitRequested
                .as_str()
                .to_string(),
            transition_source: TestnetExecutionTransitionSource::ApiSubmit
                .as_str()
                .to_string(),
            reason: Some("submit_requested".to_string()),
            payload: None,
            created_by: actor.actor_id,
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        },
        None,
        Some("SUBMIT_REQUESTED"),
        TestnetExecutionState::OrderSubmitRequested,
        None,
        None,
    )
    .await;

    let mut exchange_order = order;
    exchange_order.client_order_id = client_order_id.clone();
    telemetry().inc_exchange_testnet_request("submit_order", "attempt");
    match state.exchange_testnet.submit_order(exchange_order).await {
        Ok(ack) => {
            submit_exchange_testnet_order_success(
                state,
                actor,
                request,
                correlation_id,
                persisted,
                ack,
            )
            .await
        }
        Err(err) => {
            exchange_testnet_adapter_rejected_response(
                state,
                actor,
                request,
                correlation_id,
                "submit_order",
                err,
                persisted.symbol,
            )
            .await
        }
    }
}

async fn build_exchange_testnet_pipeline_preview(
    state: &AppState,
    risk_decision_id: Uuid,
    correlation_id: Uuid,
) -> std::result::Result<PreparedExchangeTestnetPipelinePreview, Response> {
    if state.exchange_testnet_environment != ExchangeEnvironment::Testnet {
        telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_environment");
        return Err(exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "invalid_exchange_environment",
            "Only testnet environment is allowed.",
        ));
    }

    match get_system_state(&state.db_pool).await {
        Ok(system_state) if system_state.kill_switch_enabled => {
            telemetry().inc_exchange_testnet_pipeline_run("preview_kill_switch_active");
            return Err(exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "kill_switch_active",
                "Global kill switch is active.",
            ));
        }
        Ok(_) => {}
        Err(err) => {
            error!(error = %err, "failed to load system state for testnet pipeline preview");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_read_system_state",
                    message: "System state could not be loaded.".to_string(),
                    request_id: correlation_id.to_string(),
                    correlation_id: correlation_id.to_string(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response());
        }
    }

    let risk_decision = get_risk_decision_by_id(&state.db_pool, risk_decision_id)
        .await
        .map_err(|err| {
            error!(error = %err, "failed to query risk decision for testnet pipeline preview");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decision",
                    message: "Risk decision could not be loaded.".to_string(),
                    request_id: correlation_id.to_string(),
                    correlation_id: correlation_id.to_string(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            telemetry().inc_exchange_testnet_pipeline_run("preview_risk_decision_not_found");
            exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "risk_decision_not_found",
                "risk_decision_id must reference an existing persisted risk decision.",
            )
        })?;

    if risk_decision.decision != "APPROVED" {
        telemetry().inc_exchange_testnet_pipeline_run("preview_risk_decision_not_approved");
        return Err(exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "risk_decision_not_approved",
            "risk_decision_id must reference an APPROVED risk decision.",
        ));
    }

    let rationale = serde_json::from_str::<Value>(&risk_decision.rationale).unwrap_or(Value::Null);
    let symbol = risk_decision
        .symbol
        .clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| json_string_field(&rationale, "symbol"))
        .ok_or_else(|| {
            telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_risk_context");
            exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "invalid_risk_decision_context",
                "risk_decision_id is missing symbol context.",
            )
        })?;
    let side = json_string_field(&rationale, "side")
        .and_then(|value| parse_side_to_exchange_order_side(&value))
        .ok_or_else(|| {
            telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_risk_context");
            exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "invalid_risk_decision_context",
                "risk_decision_id is missing side context.",
            )
        })?;
    let approved_notional = risk_decision
        .approved_notional
        .or_else(|| json_decimal_field(&rationale, "suggested_notional"))
        .ok_or_else(|| {
            telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_risk_context");
            exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "invalid_risk_decision_context",
                "risk_decision_id is missing approved notional context.",
            )
        })?;
    let symbol_model = Symbol::new(symbol.clone()).map_err(|_| {
        telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_symbol");
        exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "invalid_symbol",
            "risk_decision_id resolved to an invalid symbol.",
        )
    })?;
    let (reference_price, reference_price_received_at) =
        load_testnet_pipeline_reference_price(state, &symbol_model, correlation_id, &symbol)
            .await?;

    let quantity = (approved_notional / reference_price).round_dp(8);
    if quantity <= Decimal::ZERO {
        telemetry().inc_exchange_testnet_pipeline_run("preview_quantity_invalid");
        return Err(exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "invalid_preview_quantity",
            "Derived preview quantity must be positive.",
        ));
    }

    let preview = ExchangeTestnetPipelinePreview {
        strategy_id: risk_decision.strategy_id.clone(),
        signal_id: risk_decision.signal_id,
        risk_decision_id,
        symbol: symbol.clone(),
        side,
        order_type: ExchangeOrderType::Market,
        quantity,
        quote_notional: approved_notional,
        reference_price,
        reference_price_received_at,
        confirmation_text: expected_testnet_pipeline_confirmation(&symbol),
        execution_state_preview: TestnetExecutionState::OrderPrepared,
        correlation_id,
        previewed_at: Utc::now(),
    };
    let order = ExchangeOrderRequest {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: symbol_model,
        side,
        order_type: ExchangeOrderType::Market,
        time_in_force: None,
        quantity: Some(quantity),
        quote_notional: Some(approved_notional),
        limit_price: None,
        client_order_id: generate_testnet_client_order_id(correlation_id),
        recv_window_ms: None,
        risk_decision_id: Some(risk_decision_id),
    };
    order.validate().map_err(|err| {
        telemetry().inc_exchange_testnet_pipeline_run("preview_invalid_order");
        exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "invalid_exchange_order_request",
            &err.to_string(),
        )
    })?;

    Ok(PreparedExchangeTestnetPipelinePreview { preview, order })
}

async fn load_testnet_pipeline_reference_price(
    state: &AppState,
    symbol: &Symbol,
    correlation_id: Uuid,
    symbol_text: &str,
) -> std::result::Result<(Decimal, chrono::DateTime<Utc>), Response> {
    let latest_tick = get_latest_market_tick(&state.db_pool, state.market_config.exchange, symbol)
        .await
        .map_err(|err| {
            error!(
                error = %err,
                symbol = %symbol_text,
                "failed to load latest tick for testnet pipeline preview"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_market_tick",
                    message: "Latest market tick could not be loaded.".to_string(),
                    request_id: correlation_id.to_string(),
                    correlation_id: correlation_id.to_string(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        })?;

    let had_tick = latest_tick.is_some();
    if let Some(tick) = latest_tick.as_ref() {
        if tick.price <= Decimal::ZERO {
            telemetry().inc_exchange_testnet_pipeline_run("preview_market_tick_invalid");
            return Err(exchange_testnet_pipeline_rejected_response_sync(
                correlation_id,
                "invalid_market_tick",
                "Latest market tick price must be positive.",
            ));
        }

        if !is_testnet_pipeline_price_stale(
            tick.received_at,
            Utc::now(),
            state.market_config.stale_threshold,
        ) {
            return Ok((tick.price, tick.received_at));
        }
    }

    let latest_candle = list_candles(
        &state.db_pool,
        state.market_config.exchange,
        symbol,
        CandleInterval::OneMinute,
        1,
    )
    .await
    .map_err(|err| {
        error!(
            error = %err,
            symbol = %symbol_text,
            "failed to load fallback candle for testnet pipeline preview"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_market_candle",
                message: "Latest fallback candle could not be loaded.".to_string(),
                request_id: correlation_id.to_string(),
                correlation_id: correlation_id.to_string(),
                timestamp: Utc::now(),
            }),
        )
            .into_response()
    })?
    .into_iter()
    .find(|candle| candle.is_closed);

    let latest_candle = latest_candle.ok_or_else(|| {
        let result_label = if had_tick {
            "preview_market_price_stale"
        } else {
            "preview_market_price_missing"
        };
        telemetry().inc_exchange_testnet_pipeline_run(result_label);
        exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            if had_tick {
                "stale_market_price"
            } else {
                "market_price_missing"
            },
            if had_tick {
                "Latest local market price is stale; a fresh tick or candle is required before testnet pipeline preview."
            } else {
                "A fresh local market tick or closed candle is required before testnet pipeline preview."
            },
        )
    })?;

    if latest_candle.close <= Decimal::ZERO {
        telemetry().inc_exchange_testnet_pipeline_run("preview_market_candle_invalid");
        return Err(exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "invalid_market_candle",
            "Latest closed candle price must be positive.",
        ));
    }

    if is_testnet_pipeline_price_stale(
        latest_candle.close_time,
        Utc::now(),
        state.market_config.stale_threshold,
    ) {
        telemetry().inc_exchange_testnet_pipeline_run("preview_market_price_stale");
        return Err(exchange_testnet_pipeline_rejected_response_sync(
            correlation_id,
            "stale_market_price",
            "Latest local market price is stale; a fresh tick or candle is required before testnet pipeline preview.",
        ));
    }

    Ok((latest_candle.close, latest_candle.close_time))
}

fn is_testnet_pipeline_price_stale(
    priced_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    stale_threshold: std::time::Duration,
) -> bool {
    now.signed_duration_since(priced_at)
        .to_std()
        .map(|age| age > stale_threshold)
        .unwrap_or(false)
}

fn exchange_testnet_pipeline_rejected_response_sync(
    correlation_id: Uuid,
    error: &'static str,
    message: &str,
) -> Response {
    (
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error,
            message: message.to_string(),
            request_id: correlation_id.to_string(),
            correlation_id: correlation_id.to_string(),
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

fn json_string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(ToOwned::to_owned)
}

fn json_decimal_field(value: &Value, key: &str) -> Option<Decimal> {
    let field = value.get(key)?;
    match field {
        Value::String(inner) => Decimal::from_str_exact(inner).ok(),
        Value::Number(inner) => Decimal::from_str_exact(&inner.to_string()).ok(),
        _ => None,
    }
}

fn parse_side_to_exchange_order_side(value: &str) -> Option<ExchangeOrderSide> {
    match value.trim().to_ascii_lowercase().as_str() {
        "buy" => Some(ExchangeOrderSide::Buy),
        "sell" => Some(ExchangeOrderSide::Sell),
        _ => None,
    }
}

async fn cancel_exchange_testnet_order_success(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    fallback_record: ExchangeTestnetOrderRecord,
    ack: ExchangeCancelAck,
) -> Response {
    telemetry().inc_exchange_testnet_request("cancel_order", "ok");
    let local_status =
        local_testnet_status_from_exchange_state(ack.status).unwrap_or(ack.status.as_str());
    let (next_state, reason) = map_cancel_ack_to_transition(&ack);
    telemetry().inc_exchange_testnet_order(
        &fallback_record.symbol,
        &fallback_record.side,
        local_status,
    );
    let payload = ack.raw_payload.clone();
    let updated = append_testnet_lifecycle_transition(
        &state.db_pool,
        &fallback_record,
        next_state,
        TestnetExecutionTransitionSource::ExchangeCancelAck,
        Some(local_status),
        ack.exchange_order_id.as_deref(),
        reason.map(ToString::to_string),
        Some(payload),
        actor.actor_id,
        Some(correlation_id),
        false,
    )
    .await;
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        actor,
        "exchange.testnet.order.cancelled",
        &fallback_record.client_order_id,
        &json!({ "status": local_status, "exchange_order_id": ack.exchange_order_id }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.order.cancelled",
            correlation_id,
            &state.config.app_name,
            json!({ "client_order_id": fallback_record.client_order_id, "status": local_status }),
        ),
    )
    .await;

    match updated {
        Ok(Some(order_record)) => {
            let view = build_exchange_testnet_order_view(&state.db_pool, order_record)
                .await
                .unwrap_or_else(|_| exchange_testnet_order_view(fallback_record.clone()));
            (
                StatusCode::OK,
                Json(ExchangeTestnetOrderResponse {
                    order: view,
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Ok(None) | Err(_) => {
            let view = build_exchange_testnet_order_view(&state.db_pool, fallback_record.clone())
                .await
                .unwrap_or_else(|_| exchange_testnet_order_view(fallback_record));
            (
                StatusCode::OK,
                Json(ExchangeTestnetOrderResponse {
                    order: view,
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn ensure_testnet_submission_allowed(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    risk_decision_id: Uuid,
    symbol: &str,
) -> std::result::Result<(), Response> {
    if state.exchange_testnet_environment != ExchangeEnvironment::Testnet {
        return Err(exchange_testnet_rejected_response(
            state,
            actor,
            request,
            correlation_id,
            "invalid_exchange_environment",
            "Only testnet environment is allowed.",
            symbol.to_string(),
        )
        .await);
    }

    match get_system_state(&state.db_pool).await {
        Ok(system_state) if system_state.kill_switch_enabled => {
            return Err(exchange_testnet_rejected_response(
                state,
                actor,
                request,
                correlation_id,
                "kill_switch_active",
                "Global kill switch is active.",
                symbol.to_string(),
            )
            .await);
        }
        Ok(_) => {}
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load system state for exchange testnet submission"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_read_system_state",
                    message: "System state could not be loaded.".to_string(),
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response());
        }
    }

    match get_risk_decision_by_id(&state.db_pool, risk_decision_id).await {
        Ok(Some(record)) if record.decision == "APPROVED" => Ok(()),
        Ok(Some(_)) => Err(exchange_testnet_rejected_response(
            state,
            actor,
            request,
            correlation_id,
            "risk_decision_not_approved",
            "risk_decision_id must reference an APPROVED risk decision.",
            symbol.to_string(),
        )
        .await),
        Ok(None) => Err(exchange_testnet_rejected_response(
            state,
            actor,
            request,
            correlation_id,
            "risk_decision_not_found",
            "risk_decision_id must reference an existing persisted risk decision.",
            symbol.to_string(),
        )
        .await),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load risk decision for exchange testnet submission"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decision",
                    message: "Risk decision could not be loaded.".to_string(),
                    request_id: request.request_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response())
        }
    }
}

async fn exchange_testnet_pipeline_rejected_response(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    error_code: &'static str,
    message: &str,
    symbol: String,
) -> Response {
    exchange_testnet_rejected_response(
        state,
        actor,
        request,
        correlation_id,
        error_code,
        message,
        symbol,
    )
    .await
}

async fn exchange_testnet_rejected_response(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    error_code: &'static str,
    message: &str,
    symbol: String,
) -> Response {
    let _ = insert_audit_log(
        &state.db_pool,
        correlation_id,
        actor,
        "exchange.testnet.order.rejected",
        &symbol,
        &json!({ "error": error_code, "message": message }),
    )
    .await;
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.order.rejected",
            correlation_id,
            &state.config.app_name,
            json!({ "symbol": symbol, "error": error_code, "message": message }),
        ),
    )
    .await;

    (
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error: error_code,
            message: message.to_string(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn exchange_testnet_adapter_rejected_response(
    state: &AppState,
    actor: &StateActor,
    request: &RequestContext,
    correlation_id: Uuid,
    operation: &'static str,
    err: aegis_core::ExchangeError,
    symbol: String,
) -> Response {
    telemetry().inc_exchange_testnet_request(operation, "error");
    telemetry().inc_exchange_testnet_error(operation, exchange_error_kind(&err));
    exchange_testnet_rejected_response(
        state,
        actor,
        request,
        correlation_id,
        "exchange_testnet_request_rejected",
        &err.to_string(),
        symbol,
    )
    .await
}

fn exchange_testnet_error_response(
    request: &RequestContext,
    operation: &'static str,
    err: aegis_core::ExchangeError,
) -> Response {
    telemetry().inc_exchange_testnet_request(operation, "error");
    telemetry().inc_exchange_testnet_error(operation, exchange_error_kind(&err));
    let status = match err {
        aegis_core::ExchangeError::Configuration(_) => StatusCode::SERVICE_UNAVAILABLE,
        aegis_core::ExchangeError::Authentication => StatusCode::BAD_GATEWAY,
        aegis_core::ExchangeError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        aegis_core::ExchangeError::LiveEnvironmentDisabled => StatusCode::CONFLICT,
        aegis_core::ExchangeError::Validation(_) => StatusCode::BAD_REQUEST,
        aegis_core::ExchangeError::Api(_)
        | aegis_core::ExchangeError::Transport(_)
        | aegis_core::ExchangeError::Serialization(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(ErrorResponse {
            error: "exchange_testnet_request_failed",
            message: err.to_string(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

fn exchange_error_kind(err: &aegis_core::ExchangeError) -> &'static str {
    match err {
        aegis_core::ExchangeError::Configuration(_) => "configuration",
        aegis_core::ExchangeError::LiveEnvironmentDisabled => "live_disabled",
        aegis_core::ExchangeError::Validation(_) => "validation",
        aegis_core::ExchangeError::Authentication => "authentication",
        aegis_core::ExchangeError::RateLimited => "rate_limited",
        aegis_core::ExchangeError::Api(_) => "api",
        aegis_core::ExchangeError::Transport(_) => "transport",
        aegis_core::ExchangeError::Serialization(_) => "serialization",
    }
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<CreatePaperOrderRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
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

    match create_paper_order(&state.db_pool, &state.config.app_name, &actor, intent).await {
        Ok(outcome) => {
            telemetry().inc_paper_order(
                outcome.order.symbol.as_str(),
                outcome.order.status.to_ascii_lowercase().as_str(),
            );
            telemetry().inc_paper_fill(outcome.order.symbol.as_str(), "buy");
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<RunPaperPipelineRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before pipeline execution");
    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            "paper.pipeline.run",
            "paper/pipeline/run",
            &json!({ "actor_id": actor.user_id }),
        )
        .await;
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<RunBacktestRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before backtest execution");
    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            "backtest.run",
            "backtest/run",
            &json!({ "actor_id": actor.user_id }),
        )
        .await;
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

async fn run_strategy_experiment_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<RunStrategyExperimentRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before experiment execution");
    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            "strategy_experiment.run",
            "experiments/strategy/run",
            &json!({ "actor_id": actor.user_id }),
        )
        .await;
    }

    let engine = ReplayEngine::new(state.db_pool.clone(), state.config.app_name.clone());
    match engine.run_strategy_experiment(payload).await {
        Ok(execution) => (
            StatusCode::OK,
            Json(StrategyExperimentRunAcceptedResponse {
                experiment: execution.result,
                runs: execution.runs,
            }),
        )
            .into_response(),
        Err(err) => {
            let message = err.to_string();
            let status = if message.contains("invalid")
                || message.contains("unsupported")
                || message.contains("cannot be empty")
                || message.contains("must be")
                || message.contains("requires at least one candidate")
            {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to run strategy experiment"
            );

            (
                status,
                Json(ErrorResponse {
                    error: "failed_to_run_strategy_experiment",
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

async fn run_multi_timeframe_strategy_experiment_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<RunStrategyMultiTimeframeExperimentRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before experiment execution");
    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            "strategy_experiment.multi_timeframe.run",
            "experiments/strategy/multi-timeframe",
            &json!({ "actor_id": actor.user_id }),
        )
        .await;
    }

    let engine = ReplayEngine::new(state.db_pool.clone(), state.config.app_name.clone());
    match engine
        .run_multi_timeframe_strategy_experiment(payload)
        .await
    {
        Ok(execution) => (
            StatusCode::OK,
            Json(StrategyMultiTimeframeExperimentAcceptedResponse {
                comparison: execution.result,
            }),
        )
            .into_response(),
        Err(err) => {
            let message = err.to_string();
            let status = if message.contains("invalid")
                || message.contains("unsupported")
                || message.contains("cannot be empty")
                || message.contains("must be")
                || message.contains("requires at least one")
            {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to run multi timeframe strategy experiment"
            );

            (
                status,
                Json(ErrorResponse {
                    error: "failed_to_run_multi_timeframe_strategy_experiment",
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

async fn run_strategy_walk_forward_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<RunStrategyWalkForwardRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before walk-forward execution");
    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            &state_actor,
            "strategy_walk_forward.run",
            "experiments/strategy/walk-forward",
            &json!({ "actor_id": actor.user_id }),
        )
        .await;
    }

    let engine = ReplayEngine::new(state.db_pool.clone(), state.config.app_name.clone());
    match engine.run_strategy_walk_forward(payload).await {
        Ok(execution) => (
            StatusCode::OK,
            Json(StrategyWalkForwardAcceptedResponse {
                walk_forward: execution.result,
                windows: execution.windows,
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

            (
                status,
                Json(ErrorResponse {
                    error: "failed_to_run_strategy_walk_forward",
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

async fn list_strategy_walk_forward_runs_handler(
    State(state): State<AppState>,
    Query(query): Query<StrategyExperimentsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    match list_strategy_walk_forward_runs(&state.db_pool, bounded_limit(query.limit)).await {
        Ok(records) => {
            let mut walk_forwards = Vec::with_capacity(records.len());
            for record in records {
                let window_records =
                    match list_strategy_walk_forward_windows(&state.db_pool, record.id).await {
                        Ok(rows) => rows,
                        Err(err) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: "failed_to_query_strategy_walk_forward_windows",
                                    message: err.to_string(),
                                    request_id: request.request_id,
                                    correlation_id: request.correlation_id,
                                    timestamp: Utc::now(),
                                }),
                            )
                                .into_response();
                        }
                    };
                match strategy_walk_forward_result_from_records(&record, &window_records) {
                    Ok(result) => walk_forwards.push(result),
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_walk_forward",
                                message: err.to_string(),
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
                Json(StrategyWalkForwardRunsResponse {
                    walk_forwards,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_walk_forward_runs",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_walk_forward_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let walk_forward_id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_walk_forward_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_strategy_walk_forward_run(&state.db_pool, walk_forward_id).await {
        Ok(Some(record)) => {
            match list_strategy_walk_forward_windows(&state.db_pool, walk_forward_id).await {
                Ok(window_records) => {
                    match strategy_walk_forward_result_from_records(&record, &window_records) {
                        Ok(walk_forward) => (
                            StatusCode::OK,
                            Json(StrategyWalkForwardResponse {
                                walk_forward,
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response(),
                        Err(err) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_walk_forward",
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
                        error: "failed_to_query_strategy_walk_forward_windows",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "strategy_walk_forward_not_found",
                message: "Strategy walk-forward run was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_walk_forward",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_strategy_walk_forward_windows_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let walk_forward_id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_walk_forward_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match list_strategy_walk_forward_windows(&state.db_pool, walk_forward_id).await {
        Ok(records) => {
            let mut windows = Vec::with_capacity(records.len());
            for record in records {
                match strategy_walk_forward_window_from_record(&record) {
                    Ok(window) => windows.push(window),
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_walk_forward_window",
                                message: err.to_string(),
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
                Json(StrategyWalkForwardWindowsResponse {
                    windows,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_walk_forward_windows",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_strategy_experiments_handler(
    State(state): State<AppState>,
    Query(query): Query<StrategyExperimentsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    match list_strategy_experiments(&state.db_pool, bounded_limit(query.limit)).await {
        Ok(records) => {
            let mut experiments = Vec::with_capacity(records.len());
            for record in records {
                let run_records = match list_strategy_experiment_runs(&state.db_pool, record.id)
                    .await
                {
                    Ok(runs) => runs,
                    Err(err) => {
                        error!(
                            request_id = %request.request_id,
                            correlation_id = %request.correlation_id,
                            error = %err,
                            experiment_id = %record.id,
                            "failed to query strategy experiment runs"
                        );
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_query_strategy_experiment_runs",
                                message: "Failed to query strategy experiment runs.".to_string(),
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response();
                    }
                };
                match strategy_experiment_result_from_records(&record, &run_records) {
                    Ok(experiment) => experiments.push(experiment),
                    Err(err) => {
                        error!(
                            request_id = %request.request_id,
                            correlation_id = %request.correlation_id,
                            error = %err,
                            experiment_id = %record.id,
                            "failed to map strategy experiment"
                        );
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_experiment",
                                message: "Strategy experiment could not be decoded.".to_string(),
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
                Json(StrategyExperimentsResponse {
                    experiments,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_experiments",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_experiment_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let experiment_id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_experiment_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_strategy_experiment(&state.db_pool, experiment_id).await {
        Ok(Some(record)) => {
            match list_strategy_experiment_runs(&state.db_pool, experiment_id).await {
                Ok(run_records) => {
                    match strategy_experiment_result_from_records(&record, &run_records) {
                        Ok(experiment) => (
                            StatusCode::OK,
                            Json(StrategyExperimentResponse {
                                experiment,
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response(),
                        Err(err) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_experiment",
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
                        error: "failed_to_query_strategy_experiment_runs",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "strategy_experiment_not_found",
                message: "Strategy experiment was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_experiment",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_multi_timeframe_comparison_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let experiment_group_id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_experiment_group_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match list_strategy_experiments_by_group(&state.db_pool, experiment_group_id).await {
        Ok(records) if records.is_empty() => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "strategy_experiment_group_not_found",
                message: "Strategy experiment comparison was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(records) => {
            let mut experiments = Vec::with_capacity(records.len());
            for record in records {
                let run_records =
                    match list_strategy_experiment_runs(&state.db_pool, record.id).await {
                        Ok(runs) => runs,
                        Err(err) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: "failed_to_query_strategy_experiment_runs",
                                    message: err.to_string(),
                                    request_id: request.request_id,
                                    correlation_id: request.correlation_id,
                                    timestamp: Utc::now(),
                                }),
                            )
                                .into_response();
                        }
                    };

                match strategy_experiment_result_from_records(&record, &run_records) {
                    Ok(experiment) => {
                        let runs = run_records
                            .iter()
                            .map(strategy_experiment_run_from_record)
                            .collect::<Result<Vec<_>, _>>();
                        match runs {
                            Ok(runs) => experiments.push((experiment, runs)),
                            Err(err) => {
                                return (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ErrorResponse {
                                        error: "failed_to_map_strategy_experiment_run",
                                        message: err.to_string(),
                                        request_id: request.request_id,
                                        correlation_id: request.correlation_id,
                                        timestamp: Utc::now(),
                                    }),
                                )
                                    .into_response();
                            }
                        }
                    }
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_experiment",
                                message: err.to_string(),
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response();
                    }
                }
            }

            match build_multi_timeframe_result_from_experiments(experiment_group_id, experiments) {
                Ok(comparison) => (
                    StatusCode::OK,
                    Json(StrategyMultiTimeframeExperimentResponse {
                        comparison,
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_build_strategy_experiment_comparison",
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
                error: "failed_to_query_strategy_experiment_group",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_strategy_experiment_runs_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let experiment_id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_experiment_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match list_strategy_experiment_runs(&state.db_pool, experiment_id).await {
        Ok(records) => {
            let mut runs = Vec::with_capacity(records.len());
            for record in records {
                match strategy_experiment_run_from_record(&record) {
                    Ok(run) => runs.push(run),
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_strategy_experiment_run",
                                message: err.to_string(),
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
                Json(StrategyExperimentRunsResponse {
                    runs,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_experiment_runs",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

fn build_multi_timeframe_result_from_experiments(
    experiment_group_id: Uuid,
    experiments: Vec<(StrategyExperimentResult, Vec<StrategyExperimentRun>)>,
) -> anyhow::Result<StrategyMultiTimeframeExperimentResult> {
    let first = experiments
        .first()
        .map(|(experiment, _)| experiment.clone())
        .ok_or_else(|| anyhow::anyhow!("strategy experiment group is empty"))?;
    let mut timeframe_comparisons = Vec::with_capacity(experiments.len());
    let mut ranked_runs = Vec::new();

    for (experiment, runs) in &experiments {
        let required_candles = experiment
            .best_run
            .as_ref()
            .map(|run| {
                run.candidate.lookback_candles as i32
                    + run.candidate.holding_candles.unwrap_or(0) as i32
                    + 2
            })
            .unwrap_or_default();
        timeframe_comparisons.push(aegis_core::StrategyTimeframeComparison {
            candidate: aegis_core::StrategyTimeframeCandidate {
                timeframe: experiment.timeframe.clone(),
                candle_count: experiment.candle_count.unwrap_or_default(),
                required_candles,
            },
            experiment_id: Some(experiment.experiment_id),
            status: experiment.status,
            run_count: experiment.run_count,
            best_run: experiment.best_run.clone(),
            skipped_reason: experiment.skipped_reason.clone(),
            warnings: experiment.warnings.clone(),
        });

        for run in runs {
            let candle_count = experiment.candle_count.unwrap_or_default();
            let required = run.candidate.lookback_candles as i32
                + run.candidate.holding_candles.unwrap_or(0) as i32
                + 2;
            let insufficient_data_penalty = if candle_count <= required {
                Decimal::new(25, 0)
            } else if candle_count - required < required {
                Decimal::new(3, 0)
            } else if candle_count - required < required.saturating_mul(2) {
                Decimal::new(1, 0)
            } else {
                Decimal::ZERO
            };
            let overtrading_penalty = if candle_count > 0
                && run.trade_count > 0
                && ((Decimal::from(run.trade_count) / Decimal::from(candle_count))
                    * Decimal::new(100, 0))
                    >= Decimal::new(25, 0)
            {
                Decimal::new(3, 0)
            } else {
                Decimal::ZERO
            };
            ranked_runs.push(aegis_core::StrategyExperimentGlobalRankingEntry {
                timeframe: experiment.timeframe.clone(),
                experiment_id: experiment.experiment_id,
                candle_count,
                required_candles: required,
                insufficient_data_penalty,
                overtrading_penalty,
                run: run.clone(),
                warnings: experiment.warnings.clone(),
            });
        }
    }

    ranked_runs.sort_by(|left, right| {
        right
            .run
            .score
            .cmp(&left.run.score)
            .then_with(|| right.run.pnl_pct.cmp(&left.run.pnl_pct))
            .then_with(|| left.run.max_drawdown_pct.cmp(&right.run.max_drawdown_pct))
            .then_with(|| right.run.win_rate.cmp(&left.run.win_rate))
            .then_with(|| {
                left.run
                    .fee_slippage_drag_pct
                    .cmp(&right.run.fee_slippage_drag_pct)
            })
            .then_with(|| right.run.trade_count.cmp(&left.run.trade_count))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
            .then_with(|| left.run.id.cmp(&right.run.id))
    });

    let global_ranking = StrategyExperimentGlobalRanking {
        ranking_metric: aegis_core::StrategyExperimentMetric::RiskAdjustedScore,
        best_run_id: ranked_runs.first().map(|entry| entry.run.id),
        ranked_runs,
    };
    let warnings = {
        let mut warnings = Vec::new();
        if timeframe_comparisons
            .iter()
            .any(|comparison| comparison.skipped_reason.is_some())
        {
            warnings.push("skipped_timeframes_present".to_string());
        }
        if global_ranking.ranked_runs.iter().any(|entry| {
            entry
                .run
                .warnings
                .iter()
                .any(|warning| warning == "overtrading_warning")
        }) {
            warnings.push("overtrading_candidates_present".to_string());
        }
        warnings
    };

    Ok(StrategyMultiTimeframeExperimentResult {
        experiment_group_id,
        strategy_id: first.strategy_id,
        symbol: first.symbol,
        requested_timeframes: experiments
            .iter()
            .map(|(experiment, _)| experiment.timeframe.clone())
            .collect(),
        start_time: first.start_time,
        end_time: first.end_time,
        initial_capital: first.initial_capital,
        fee_bps: first.fee_bps,
        slippage_bps: first.slippage_bps,
        max_signal_age_ms: first.max_signal_age_ms,
        max_runs: first.max_runs,
        status: if global_ranking.ranked_runs.is_empty() {
            aegis_core::StrategyExperimentStatus::Failed
        } else {
            aegis_core::StrategyExperimentStatus::Completed
        },
        timeframe_comparisons,
        global_ranking,
        warnings,
        created_at: first.created_at,
        correlation_id: first.correlation_id,
    })
}

async fn get_strategy_performance_handler(
    State(state): State<AppState>,
    Query(query): Query<StrategyAnalyticsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("strategy_performance");
    let performance_request = strategy_performance_request_from_query(query);

    match get_strategy_performance_summary(&state.db_pool, &performance_request).await {
        Ok(summary) => (
            StatusCode::OK,
            Json(StrategyPerformanceSummaryResponse {
                summary,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_strategy_performance",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_strategy_performance_rankings_handler(
    State(state): State<AppState>,
    Query(query): Query<StrategyAnalyticsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("strategy_rankings");
    let performance_request = strategy_performance_request_from_query(query);

    match list_strategy_performance_rankings(&state.db_pool, &performance_request).await {
        Ok(rankings) => (
            StatusCode::OK,
            Json(StrategyPerformanceRankingsResponse {
                rankings,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_strategy_rankings",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_decision_breakdown_handler(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
    Query(query): Query<StrategyDecisionBreakdownQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("strategy_decision_breakdown");
    let performance_request = StrategyPerformanceRequest {
        strategy_id: Some(strategy_id),
        symbol: query.symbol,
        timeframe: query.timeframe,
        mode: StrategyPerformanceMode::Shadow,
        start_time: query.start_time,
        end_time: query.end_time,
        limit: None,
    };

    match get_strategy_shadow_decision_breakdown(&state.db_pool, &performance_request).await {
        Ok(breakdown) => (
            StatusCode::OK,
            Json(StrategyDecisionBreakdownResponse {
                breakdown,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_strategy_decision_breakdown",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_paper_pnl_breakdown_handler(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
    Query(query): Query<StrategyDecisionBreakdownQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("strategy_paper_pnl_breakdown");
    let performance_request = StrategyPerformanceRequest {
        strategy_id: Some(strategy_id),
        symbol: query.symbol,
        timeframe: query.timeframe,
        mode: StrategyPerformanceMode::Paper,
        start_time: query.start_time,
        end_time: query.end_time,
        limit: None,
    };

    match get_strategy_paper_pnl_breakdown(&state.db_pool, &performance_request).await {
        Ok(breakdown) => (
            StatusCode::OK,
            Json(StrategyPnlBreakdownResponse {
                breakdown,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_strategy_paper_pnl_breakdown",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_backtest_breakdown_handler(
    State(state): State<AppState>,
    Path(strategy_id): Path<String>,
    Query(query): Query<StrategyDecisionBreakdownQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("strategy_backtest_breakdown");
    let performance_request = StrategyPerformanceRequest {
        strategy_id: Some(strategy_id),
        symbol: query.symbol,
        timeframe: query.timeframe,
        mode: StrategyPerformanceMode::Backtest,
        start_time: query.start_time,
        end_time: query.end_time,
        limit: None,
    };

    match get_strategy_backtest_breakdown(&state.db_pool, &performance_request).await {
        Ok(breakdown) => (
            StatusCode::OK,
            Json(StrategyPnlBreakdownResponse {
                breakdown,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_strategy_backtest_breakdown",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_testnet_promotion_funnel_handler(
    State(state): State<AppState>,
    Query(query): Query<TestnetPromotionFunnelQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("testnet_promotion_funnel");
    telemetry().inc_analytics_promotion_funnel_request();
    let funnel_request = testnet_promotion_funnel_request_from_query(query);

    match get_testnet_promotion_funnel_summary(&state.db_pool, &funnel_request).await {
        Ok(summary) => (
            StatusCode::OK,
            Json(TestnetPromotionFunnelSummaryResponse {
                summary,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_testnet_promotion_funnel",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_testnet_promotion_funnel_outcomes_handler(
    State(state): State<AppState>,
    Query(query): Query<TestnetPromotionFunnelQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("testnet_promotion_funnel_outcomes");
    telemetry().inc_analytics_promotion_funnel_request();
    let funnel_request = testnet_promotion_funnel_request_from_query(query);

    match (
        get_testnet_promotion_outcome_breakdown(&state.db_pool, &funnel_request).await,
        get_testnet_promotion_lifecycle_breakdown(&state.db_pool, &funnel_request).await,
    ) {
        (Ok(outcomes), Ok(lifecycle)) => (
            StatusCode::OK,
            Json(TestnetPromotionFunnelOutcomesResponse {
                outcomes,
                lifecycle,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        (Err(err), _) | (_, Err(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_testnet_promotion_outcomes",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_testnet_promotion_funnel_rows_handler(
    State(state): State<AppState>,
    Query(query): Query<TestnetPromotionFunnelQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    telemetry().inc_analytics_request("testnet_promotion_funnel_rows");
    telemetry().inc_analytics_promotion_funnel_request();
    let funnel_request = testnet_promotion_funnel_request_from_query(query);

    match list_testnet_promotion_funnel_rows(&state.db_pool, &funnel_request).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(TestnetPromotionFunnelRowsResponse {
                rows,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_testnet_promotion_rows",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn generate_operator_report_handler(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<OperatorReportRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);

    if payload.persist && !operator_reports::persist_allowed(actor.as_ref()) {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "operator_report_persist_forbidden",
                message: "Persisting operator reports requires OPERATOR or OWNER.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    if let Err(err) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_operator_report_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    match operator_reports::generate_operator_report(&state, &payload, actor.as_ref()).await {
        Ok(report) => (
            StatusCode::OK,
            Json(OperatorReportResponse {
                report,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_generate_operator_report",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_operator_reports_handler(
    State(state): State<AppState>,
    Query(query): Query<OperatorReportsListQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match operator_reports::list_operator_reports(
        &state.db_pool,
        operator_reports::bounded_report_list_limit(query.limit),
    )
    .await
    {
        Ok(reports) => (
            StatusCode::OK,
            Json(OperatorReportsListResponse {
                reports,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_list_operator_reports",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_operator_report_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match operator_reports::get_operator_report(&state.db_pool, id).await {
        Ok(Some(report)) => (
            StatusCode::OK,
            Json(OperatorReportResponse {
                report,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "operator_report_not_found",
                message: "Operator report was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_operator_report",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn check_execution_readiness_handler(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<ExecutionReadinessRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);

    if payload.persist && !readiness::persist_allowed(actor.as_ref()) {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "readiness_snapshot_persist_forbidden",
                message: "Persisting readiness snapshots requires OPERATOR or OWNER.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    match readiness::compute_execution_readiness(&state, &payload, actor.as_ref()).await {
        Ok(readiness) => (
            StatusCode::OK,
            Json(ExecutionReadinessResponse {
                readiness,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "failed_to_compute_execution_readiness",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn list_execution_readiness_snapshots_handler(
    State(state): State<AppState>,
    Query(query): Query<ExecutionReadinessSnapshotsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match readiness::list_snapshots(&state, readiness::bounded_snapshot_list_limit(query.limit))
        .await
    {
        Ok(snapshots) => (
            StatusCode::OK,
            Json(ExecutionReadinessSnapshotsResponse {
                snapshots,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_list_execution_readiness_snapshots",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_execution_readiness_snapshot_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match readiness::get_snapshot(&state, id).await {
        Ok(Some(snapshot)) => (
            StatusCode::OK,
            Json(ExecutionReadinessSnapshotsResponse {
                snapshots: vec![snapshot],
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "execution_readiness_snapshot_not_found",
                message: "Execution readiness snapshot was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_load_execution_readiness_snapshot",
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
    let status_filter = match query
        .status
        .as_deref()
        .unwrap_or("all")
        .parse::<PaperPositionStatusFilter>()
    {
        Ok(filter) => filter,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_paper_position_status",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => {
            match list_paper_positions(
                &state.db_pool,
                account.id,
                status_filter,
                bounded_paper_limit(query.limit),
            )
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

async fn close_paper_position_handler(
    State(state): State<AppState>,
    Path(position_id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(payload): Json<PaperClosePositionPayload>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = required_state_actor(actor);
    let reason = match payload.reason {
        Some(reason) => match reason.parse::<PaperCloseReason>() {
            Ok(reason) => Some(reason),
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_paper_close_reason",
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
    let close_mode = match payload.close_mode {
        Some(mode) => match mode.parse::<PaperCloseMode>() {
            Ok(mode) => mode,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_paper_close_mode",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        },
        None => PaperCloseMode::MarketSimulated,
    };

    match close_paper_position(
        &state.db_pool,
        &state.market_config,
        &actor,
        PaperClosePositionRequest {
            position_id,
            confirmation_text: payload.confirmation_text,
            reason,
            close_mode,
            correlation_id: payload.correlation_id,
            allow_stale_price: payload.allow_stale_price.unwrap_or(false),
        },
    )
    .await
    {
        Ok(summary) => {
            telemetry().inc_paper_position_close(summary.symbol.as_str(), summary.status.as_str());
            telemetry().inc_paper_fill(summary.symbol.as_str(), "sell");
            (
                StatusCode::OK,
                Json(paper_close_position_view(summary, request.request_id)),
            )
                .into_response()
        }
        Err(ClosePaperPositionError::Validation(issue)) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: issue.as_str(),
                message: issue.as_str().replace('_', " "),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(ClosePaperPositionError::Unexpected(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_close_paper_position",
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
        &EventEnvelope::new(
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
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<CandleBackfillRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Uuid::parse_str(&request.correlation_id).ok();
    }
    let correlation_id = payload
        .correlation_id
        .expect("correlation_id must be set before backfill execution");
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
        Ok(result) => {
            if let Some(actor) = actor.as_ref() {
                let state_actor = state_actor_from_authenticated(actor);
                let _ = insert_audit_log(
                    &state.db_pool,
                    correlation_id,
                    &state_actor,
                    "market.backfill.run",
                    "market/backfill/candles",
                    &json!({
                        "actor_id": actor.user_id,
                        "run_id": result.run_id,
                        "symbol": result.symbol,
                        "interval": result.interval
                    }),
                )
                .await;
            }
            (StatusCode::OK, Json(result)).into_response()
        }
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

async fn post_market_aggregate_candles(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<CandleAggregationRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Uuid::parse_str(&request.correlation_id).ok();
    }
    if let Err(err) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_candle_aggregation_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let symbol = match payload.normalized_symbol() {
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
    let target_interval = match payload.target_interval.parse::<CandleInterval>() {
        Ok(interval) => interval,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_target_interval",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let source_candles = match get_closed_1m_candles_range(
        &state.db_pool,
        payload.exchange,
        &symbol,
        payload.start_time,
        payload.end_time,
    )
    .await
    {
        Ok(candles) => candles,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                symbol = %symbol,
                target_interval = %target_interval.as_str(),
                "failed to load source candles for aggregation"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_source_candles",
                    message: "Failed to load source candles.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let aggregated = aggregate_closed_1m_candles(&source_candles, target_interval);
    let upsert = match upsert_aggregated_candles(&state.db_pool, &aggregated.candles).await {
        Ok(result) => result,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                symbol = %symbol,
                target_interval = %target_interval.as_str(),
                "failed to persist aggregated candles"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_persist_aggregated_candles",
                    message: "Failed to persist aggregated candles.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let response = CandleAggregationResult {
        exchange: payload.exchange,
        symbol: symbol.as_str().to_string(),
        source_interval: payload.source_interval,
        target_interval: payload.target_interval,
        start_time: payload.start_time,
        end_time: payload.end_time,
        source_candles: i32::try_from(source_candles.len()).unwrap_or(i32::MAX),
        aggregated_candles: i32::try_from(aggregated.candles.len()).unwrap_or(i32::MAX),
        inserted: upsert.inserted_candles,
        updated: upsert.updated_candles,
        skipped_incomplete: aggregated.skipped_incomplete_buckets,
        correlation_id: payload.correlation_id,
    };

    if let Some(actor) = actor.as_ref() {
        let state_actor = state_actor_from_authenticated(actor);
        let _ = insert_audit_log(
            &state.db_pool,
            response.correlation_id.unwrap_or_else(Uuid::new_v4),
            &state_actor,
            "market.candles.aggregate",
            "market/candles/aggregate",
            &json!({
                "actor_id": actor.user_id,
                "exchange": response.exchange.as_str(),
                "symbol": &response.symbol,
                "source_interval": &response.source_interval,
                "target_interval": &response.target_interval,
                "source_candles": response.source_candles,
                "aggregated_candles": response.aggregated_candles,
                "inserted": response.inserted,
                "updated": response.updated,
                "skipped_incomplete": response.skipped_incomplete
            }),
        )
        .await;
    }

    (StatusCode::OK, Json(response)).into_response()
}

async fn get_market_candle_coverage(
    State(state): State<AppState>,
    Query(query): Query<CandleCoverageQuery>,
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

    match get_aggregated_candle_coverage(&state.db_pool, state.market_config.exchange, &symbol)
        .await
    {
        Ok(coverage) => (
            StatusCode::OK,
            Json(CandleCoverageResponse {
                coverage,
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
                "failed to query candle coverage"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_candle_coverage",
                    message: "Failed to query candle coverage.".to_string(),
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

async fn get_research_data_coverage(
    State(state): State<AppState>,
    Query(query): Query<ResearchDataCoverageQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let exchange = match query
        .exchange
        .as_deref()
        .unwrap_or("binance")
        .parse::<aegis_core::MarketDataSource>()
    {
        Ok(exchange) => exchange,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_exchange",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };
    let payload = ResearchDataCoverageRequest {
        exchange,
        symbol: query.symbol,
        intervals: query
            .intervals
            .split(',')
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect(),
        start_time: query.start_time,
        end_time: query.end_time,
        required_coverage_pct: query.required_coverage_pct.unwrap_or(Decimal::new(95, 0)),
        correlation_id: Uuid::parse_str(&request.correlation_id).ok(),
    };
    if let Err(err) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_research_data_coverage_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let service = ResearchDatasetService::new(
        state.db_pool.clone(),
        state.config.app_name.clone(),
        state.market_config.binance_rest_base_url.clone(),
    );
    match service.inspect_coverage(payload).await {
        Ok(coverage) => (
            StatusCode::OK,
            Json(ResearchDataCoverageResponse {
                coverage,
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
                "failed to inspect research data coverage"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_inspect_research_data_coverage",
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

async fn post_research_data_build(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<ResearchDatasetBuildRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Uuid::parse_str(&request.correlation_id).ok();
    }
    if let Err(err) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_research_dataset_build_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let service = ResearchDatasetService::new(
        state.db_pool.clone(),
        state.config.app_name.clone(),
        state.market_config.binance_rest_base_url.clone(),
    );
    match service.build_dataset(payload).await {
        Ok(build) => {
            if let Some(actor) = actor.as_ref() {
                let state_actor = state_actor_from_authenticated(actor);
                let _ = insert_audit_log(
                    &state.db_pool,
                    build.correlation_id,
                    &state_actor,
                    "research.dataset.build.run",
                    "research/data/build",
                    &json!({
                        "actor_id": actor.user_id,
                        "build_id": build.build_id,
                        "symbol": build.symbol,
                        "status": build.status.as_str(),
                    }),
                )
                .await;
            }
            (
                StatusCode::OK,
                Json(ResearchDatasetBuildResponse {
                    build,
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
                "failed to build research dataset"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_build_research_dataset",
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

async fn list_research_data_builds(
    State(state): State<AppState>,
    Query(query): Query<ResearchDatasetBuildsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let service = ResearchDatasetService::new(
        state.db_pool.clone(),
        state.config.app_name.clone(),
        state.market_config.binance_rest_base_url.clone(),
    );
    match service
        .list_builds(bounded_backfill_runs_limit(query.limit))
        .await
    {
        Ok(builds) => (
            StatusCode::OK,
            Json(ResearchDatasetBuildsResponse {
                builds,
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
                "failed to list research dataset builds"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_research_dataset_builds",
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

async fn get_research_data_build(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let service = ResearchDatasetService::new(
        state.db_pool.clone(),
        state.config.app_name.clone(),
        state.market_config.binance_rest_base_url.clone(),
    );
    match service.get_build(id).await {
        Ok(Some(build)) => (
            StatusCode::OK,
            Json(ResearchDatasetBuildResponse {
                build,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "research_dataset_build_not_found",
                message: "Research dataset build was not found.".to_string(),
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
                build_id = %id,
                "failed to query research dataset build"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_research_dataset_build",
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

async fn get_strategy_config_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    get_strategy_by_id(State(state), Path(id), request).await
}

async fn validate_strategy_config_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    Json(mut payload): Json<StrategyConfigUpdateRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    payload.strategy_id = id.clone();
    let validation = validate_strategy_config(&payload, &strategy_validation_context(&state));
    telemetry().inc_strategy_config_validation(
        payload.strategy_id.as_str(),
        if validation.valid {
            "valid"
        } else {
            "rejected"
        },
    );
    let event_type = if validation.valid {
        "strategy.config.validated"
    } else {
        "strategy.config.rejected"
    };
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            request
                .correlation_id
                .parse()
                .unwrap_or_else(|_| Uuid::new_v4()),
            state.config.app_name.clone(),
            json!({
                "strategy_id": payload.strategy_id,
                "issues": validation.issues,
            }),
        ),
    )
    .await;

    let status = if validation.valid {
        StatusCode::OK
    } else {
        StatusCode::UNPROCESSABLE_ENTITY
    };
    (
        status,
        Json(StrategyConfigValidationResponse {
            validation,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
}

async fn update_strategy_config_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
    Json(mut payload): Json<StrategyConfigUpdateRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let actor = current_actor(actor);
    let actor_id = actor.as_ref().map(|value| value.user_id);
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
    payload.strategy_id = id;
    let validation = validate_strategy_config(&payload, &strategy_validation_context(&state));
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let current_config = ensure_strategy_config(&state, strategy_id).await.ok();
    telemetry().inc_strategy_config_validation(
        strategy_id.as_str(),
        if validation.valid {
            "valid"
        } else {
            "rejected"
        },
    );

    if !validation.valid {
        telemetry().inc_strategy_config_update(strategy_id.as_str(), "rejected");
        let _ = insert_strategy_config_audit(
            &state.db_pool,
            &StrategyConfigAuditEntry {
                audit_id: Uuid::new_v4(),
                strategy_id: strategy_id.to_string(),
                version: None,
                old_config: current_config.clone(),
                new_config: None,
                validation_issues: validation.issues.clone(),
                actor_id,
                correlation_id,
                created_at: Utc::now(),
            },
        )
        .await;
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "strategy.config.rejected",
                correlation_id,
                state.config.app_name.clone(),
                json!({
                    "strategy_id": strategy_id,
                    "issues": validation.issues,
                    "actor_id": actor_id,
                }),
            ),
        )
        .await;
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(StrategyConfigValidationResponse {
                validation,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let config = validation
        .normalized_config
        .clone()
        .expect("valid config must be present");
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "strategy.config.validated",
            correlation_id,
            state.config.app_name.clone(),
            json!({ "strategy_id": strategy_id, "actor_id": actor_id }),
        ),
    )
    .await;

    match persist_strategy_config_version(&state.db_pool, &config, actor_id, correlation_id).await {
        Ok(_) => {
            telemetry().inc_strategy_config_update(strategy_id.as_str(), "updated");
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "strategy.config.updated",
                    correlation_id,
                    state.config.app_name.clone(),
                    json!({ "strategy_id": strategy_id, "actor_id": actor_id }),
                ),
            )
            .await;
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
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_strategy_status",
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
                error: "failed_to_update_strategy_config",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_config_versions_handler(
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

    match list_strategy_config_versions(&state.db_pool, strategy_id).await {
        Ok(records) => (
            StatusCode::OK,
            Json(StrategyConfigVersionsResponse {
                versions: records
                    .iter()
                    .map(strategy_config_version_from_record)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_default(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_config_versions",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_strategy_config_audit_handler(
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

    match list_strategy_config_audit(&state.db_pool, strategy_id).await {
        Ok(records) => (
            StatusCode::OK,
            Json(StrategyConfigAuditResponse {
                audit: records
                    .iter()
                    .map(strategy_config_audit_from_record)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_default(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_strategy_config_audit",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn strategy_dry_run_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<StrategyDryRunRequest>,
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

    let persisted = match ensure_strategy_config(&state, strategy_id).await {
        Ok(config) => config,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_strategy_config",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let validation = if let Some(override_request) = payload.config_override.clone() {
        validate_strategy_config(&override_request, &strategy_validation_context(&state))
    } else {
        validate_strategy_config(
            &strategy_update_request_from_config(&persisted),
            &strategy_validation_context(&state),
        )
    };
    let symbol = match payload
        .symbol
        .clone()
        .map(Symbol::new)
        .transpose()
        .map_err(|err| err.to_string())
    {
        Ok(Some(symbol)) => symbol,
        Ok(None) => default_strategy_symbol(&persisted),
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let config = validation
        .normalized_config
        .clone()
        .unwrap_or_else(|| persisted.clone());
    let timeframe = if let Some(raw) = payload.timeframe.as_deref() {
        match raw.parse::<CandleInterval>() {
            Ok(timeframe) => timeframe,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_timeframe",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        config.timeframe
    };
    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));

    let result = if !validation.valid {
        telemetry().inc_strategy_dry_run(strategy_id.as_str(), "rejected");
        StrategyDryRunResult {
            strategy_id: strategy_id.to_string(),
            symbol: symbol.as_str().to_string(),
            timeframe: timeframe.as_str().to_string(),
            config_valid: false,
            validation_issues: validation.issues.clone(),
            would_generate_signal: false,
            reason: "config_invalid".to_string(),
            source_candle_open_time: None,
            confidence: None,
            correlation_id,
            evaluated_at: Utc::now(),
        }
    } else {
        let candles = match get_recent_closed_candles(
            &state.db_pool,
            &symbol,
            timeframe,
            required_candle_count(&config),
        )
        .await
        {
            Ok(candles) => candles,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_closed_candles",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        };
        let evaluation = match evaluate_strategy(StrategyEvaluationContext {
            correlation_id,
            strategy_id,
            symbol: symbol.clone(),
            config: StrategyConfig {
                timeframe,
                ..config.clone()
            },
            candles,
            evaluated_at: Utc::now(),
        }) {
            Ok(evaluation) => evaluation,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_evaluate_strategy",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        };
        telemetry().inc_strategy_dry_run(
            strategy_id.as_str(),
            if evaluation.generated {
                "signal"
            } else {
                "no_signal"
            },
        );
        StrategyDryRunResult {
            strategy_id: strategy_id.to_string(),
            symbol: symbol.as_str().to_string(),
            timeframe: timeframe.as_str().to_string(),
            config_valid: true,
            validation_issues: validation.issues.clone(),
            would_generate_signal: evaluation.generated,
            reason: evaluation.reason.as_str().to_string(),
            source_candle_open_time: evaluation
                .signal
                .as_ref()
                .map(|signal| signal.source_candle_open_time),
            confidence: evaluation
                .signal
                .as_ref()
                .map(|signal| signal.confidence.value),
            correlation_id,
            evaluated_at: evaluation.evaluated_at,
        }
    };

    (
        StatusCode::OK,
        Json(StrategyDryRunResponse {
            result,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn enable_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> impl IntoResponse {
    toggle_strategy_status(state, id, StrategyStatus::Enabled, request, actor).await
}

async fn strategy_diagnostics_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<StrategyDiagnosticsQuery>,
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

    let persisted = match ensure_strategy_config(&state, strategy_id).await {
        Ok(config) => config,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_strategy_config",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let symbol = match query
        .symbol
        .clone()
        .map(Symbol::new)
        .transpose()
        .map_err(|err| err.to_string())
    {
        Ok(Some(symbol)) => symbol,
        Ok(None) => default_strategy_symbol(&persisted),
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let timeframe = if let Some(raw) = query.timeframe.as_deref() {
        match raw.parse::<CandleInterval>() {
            Ok(timeframe) => timeframe,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_timeframe",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        persisted.timeframe
    };

    let limit = bounded_limit(query.limit);
    let validation = validate_strategy_config(
        &strategy_update_request_from_config(&persisted),
        &strategy_validation_context(&state),
    );
    let correlation_id = parse_correlation_id(&request.correlation_id);
    let candle_limit = required_candle_count(&persisted).max(limit);
    let candles =
        match get_recent_closed_candles(&state.db_pool, &symbol, timeframe, candle_limit).await {
            Ok(candles) => candles,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_closed_candles",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        };
    let evaluated_at = Utc::now();

    let mut result = if !validation.valid {
        StrategyDiagnosticsResult {
            strategy_id: strategy_id.to_string(),
            symbol: symbol.as_str().to_string(),
            timeframe: timeframe.as_str().to_string(),
            strategy_enabled: persisted.enabled,
            config_valid: false,
            validation_issues: validation.issues.clone(),
            data_health: build_data_health(&persisted, &candles, evaluated_at, limit),
            condition_checks: vec![StrategyDiagnosticCheck {
                name: "config_validation".to_string(),
                passed: false,
                severity: aegis_core::StrategyDiagnosticSeverity::Error,
                message: format!(
                    "Strategy config has {} validation issue(s).",
                    validation.issues.len()
                ),
                actual: Some(validation.issues.len().to_string()),
                expected: Some("0".to_string()),
            }],
            final_decision: StrategyDiagnosticsDecision::InvalidConfig,
            no_signal_reason: Some(StrategyNoSignalReason::InvalidConfig),
            summary: format!(
                "Strategy config is invalid: {}",
                validation
                    .issues
                    .iter()
                    .map(|issue| issue.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            source_candle_open_time: None,
            confidence: None,
            correlation_id,
            evaluated_at,
        }
    } else {
        match diagnose_strategy(StrategyEvaluationContext {
            correlation_id,
            strategy_id,
            symbol: symbol.clone(),
            config: StrategyConfig {
                timeframe,
                ..persisted.clone()
            },
            candles,
            evaluated_at,
        }) {
            Ok(result) => result,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_evaluate_strategy_diagnostics",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        }
    };
    result.data_health.latest_closes = result
        .data_health
        .latest_closes
        .into_iter()
        .rev()
        .take(limit as usize)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    (
        StatusCode::OK,
        Json(StrategyDiagnosticsResponse {
            result,
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn disable_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> impl IntoResponse {
    toggle_strategy_status(state, id, StrategyStatus::Disabled, request, actor).await
}

async fn toggle_strategy_status(
    state: AppState,
    id: String,
    status: StrategyStatus,
    request: Option<Extension<RequestContext>>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> Response {
    let request = request_context(request);
    let actor = current_actor(actor);
    let actor_id = actor.as_ref().map(|value| value.user_id);
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
    config.enabled = status == StrategyStatus::Enabled;

    let correlation_id = parse_correlation_id(&request.correlation_id);
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "strategy.config.validated",
            correlation_id,
            state.config.app_name.clone(),
            json!({ "strategy_id": strategy_id, "actor_id": actor_id }),
        ),
    )
    .await;

    match persist_strategy_config_version(&state.db_pool, &config, actor_id, correlation_id).await {
        Ok(_) => match get_strategy_status(&state.db_pool, strategy_id).await {
            Ok(Some(strategy)) => {
                let _ = insert_system_event(
                    &state.db_pool,
                    &EventEnvelope::new(
                        "strategy.config.updated",
                        correlation_id,
                        state.config.app_name.clone(),
                        json!({ "strategy_id": strategy_id, "actor_id": actor_id }),
                    ),
                )
                .await;
                (
                    StatusCode::OK,
                    Json(StrategyToggleResponse {
                        strategy: strategy_status_view(strategy),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
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
    let required_candles = strategy_engine::required_candle_count(&config);
    let candles = match get_recent_closed_candles(
        &state.db_pool,
        &symbol,
        config.timeframe,
        required_candles,
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
        telemetry().inc_strategy_evaluation(id.as_str(), symbol.as_str(), "signal_generated");
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
            telemetry().inc_strategy_signal(id.as_str(), symbol.as_str(), signal.side.as_str());
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
    telemetry().inc_strategy_evaluation(id.as_str(), symbol.as_str(), "no_signal");

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
        bootstrap_owner, bounded_recent_events_limit, bounded_risk_decisions_limit,
        build_cors_layer, cancel_exchange_testnet_order, check_execution_readiness_handler,
        generate_testnet_client_order_id, get_exchange_testnet_shadow_promotion_handler,
        get_exchange_testnet_shadow_run_handler, get_execution_readiness_snapshot_handler,
        get_risk_decisions, get_strategy_list, is_valid_resume_confirmation,
        is_valid_testnet_order_confirmation, list_exchange_testnet_order_repairs,
        list_exchange_testnet_shadow_promotions_handler, list_exchange_testnet_shadow_runs_handler,
        list_execution_readiness_snapshots_handler, login, logout, metrics, normalize_route_label,
        order_view, parse_correlation_id_filter, parse_cors_allowed_origins, parse_order_intent,
        parse_risk_check_context, preview_exchange_testnet_pipeline,
        preview_exchange_testnet_shadow_promotion_handler,
        reconcile_exchange_testnet_orders_handler, reconcile_testnet_orders, refresh,
        repair_exchange_testnet_order, request_context_middleware, risk_decision_not_found_error,
        route_access, run_exchange_testnet_shadow_handler, strategy_diagnostics_handler,
        submit_exchange_testnet_pipeline, submit_exchange_testnet_shadow_promotion_handler,
        AppConfig, AppState, ExchangeTestnetPipelinePreviewResponse, ExecutionReadinessResponse,
        ExecutionReadinessSnapshotsResponse, RequestContext, StrategyRuntimeConfig,
        TestnetShadowPromotionResponse, TestnetShadowPromotionSubmitResponse,
        TestnetShadowPromotionsResponse, TestnetShadowRunResponse, TestnetShadowRunsResponse,
        CLI_AUTH_MODE_HEADER, CLI_AUTH_MODE_VALUE, DEFAULT_RECENT_EVENTS_LIMIT,
        DEFAULT_RISK_DECISIONS_LIMIT, MAX_RECENT_EVENTS_LIMIT, MAX_RISK_DECISIONS_LIMIT,
    };
    use crate::auth::{decode_access_token, hash_password, AuthConfig};
    use crate::{CreatePaperOrderRequest, RiskEvaluateRequest};
    use aegis_core::{
        expected_testnet_pipeline_confirmation, expected_testnet_shadow_promotion_confirmation,
        AuthLoginResponse, AuthLogoutResponse, AuthRefreshResponse, AuthUserResponse, Candle,
        CandleInterval, DataFreshnessStatus, ExchangeEnvironment, ExchangeOrderState,
        ExecutionReadinessCheckSeverity, ExecutionReadinessRecommendation,
        ExecutionReadinessStatus, ExecutionReadinessTarget, FeedStatus, MarketDataSource,
        MarketMode, MarketTick, PaperAccount, PaperAccountStatus, RiskConfig, Side, StrategyConfig,
        StrategyId, StrategyMode, Symbol, TestnetExecutionState, TestnetRepairAction,
        TestnetShadowDecision, TestnetShadowPromotionStatus, TestnetShadowPromotionSubmitRequest,
        TestnetShadowRunnerState, TestnetShadowRunnerStatus, UserRole, UserStatus,
    };
    use axum::{
        body::Body,
        http::{
            header::{
                self, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION,
                ORIGIN, SET_COOKIE,
            },
            HeaderValue, Request, StatusCode,
        },
        middleware,
        routing::{get, post},
        Json, Router,
    };
    use chrono::{TimeZone, Utc};
    use db::{
        count_users, get_exchange_testnet_order_by_client_order_id, get_session_by_id,
        get_user_by_email, insert_exchange_reconciliation_run, insert_exchange_testnet_order,
        insert_exchange_testnet_repair_action, insert_market_tick, insert_paper_account,
        insert_testnet_shadow_run, insert_user, list_exchange_reconciliation_mismatches,
        list_exchange_reconciliation_runs, list_exchange_testnet_order_lifecycle_events,
        list_exchange_testnet_orders, list_exchange_testnet_repair_actions, list_orders,
        list_paper_equity_snapshots, list_paper_positions, list_paper_trade_journal,
        set_kill_switch_state, test_support::TestDatabase, upsert_candle,
        upsert_exchange_private_stream_state, upsert_market_feed_status, upsert_risk_config,
        upsert_strategy_config, upsert_testnet_shadow_runner_state,
        ExchangePrivateStreamStateRecord, ExchangeReconciliationRunRecord,
        ExchangeTestnetOrderRecord, OrderRecord, PgPool, StateActor, TestnetShadowRunRecord,
    };
    use exchange::{
        testing::{FakeExchangeAdapter, FakeOrderStatus, FakeSubmitAck},
        BinanceSpotTestnetAdapter, BinanceSpotTestnetConfig,
    };
    use market_ingest::MarketIngestConfig;
    use rust_decimal::Decimal;
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};
    use sqlx::Row;
    use std::sync::Arc;
    use telemetry::telemetry;
    use tower::util::ServiceExt;
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
    fn cors_allowed_origins_default_to_local_dashboard_hosts() {
        let origins = parse_cors_allowed_origins(None).expect("default origins");

        assert_eq!(
            origins,
            vec![
                HeaderValue::from_static("http://localhost:3001"),
                HeaderValue::from_static("http://127.0.0.1:3001"),
            ]
        );
    }

    #[test]
    fn cors_allowed_origins_parse_and_dedupe() {
        let origins = parse_cors_allowed_origins(Some(
            " http://localhost:3001,https://aegis.anggaprytn.com,http://localhost:3001 ",
        ))
        .expect("parsed origins");

        assert_eq!(
            origins,
            vec![
                HeaderValue::from_static("http://localhost:3001"),
                HeaderValue::from_static("https://aegis.anggaprytn.com"),
            ]
        );
    }

    #[test]
    fn cors_allowed_origins_reject_wildcard() {
        let error = parse_cors_allowed_origins(Some("*")).expect_err("wildcard rejected");

        assert!(error.contains("cannot contain '*'"));
    }

    #[tokio::test]
    async fn cors_preflight_allows_configured_dashboard_origin() {
        let app = Router::new()
            .route("/auth/login", post(|| async { StatusCode::OK }))
            .layer(build_cors_layer(&AppConfig {
                app_name: "aegis-test-api".to_string(),
                environment: "test".to_string(),
                bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
                database_url: "postgres://unused".to_string(),
                database_max_connections: 5,
                cors_allowed_origins: parse_cors_allowed_origins(None).expect("origins"),
            }));

        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/auth/login")
                    .header(ORIGIN, "http://localhost:3001")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type,authorization")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://localhost:3001"))
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
            Some(&HeaderValue::from_static("true"))
        );
    }

    #[test]
    fn resume_confirmation_must_match_exact_phrase() {
        assert!(is_valid_resume_confirmation("RESUME TRADING"));
        assert!(!is_valid_resume_confirmation("resume trading"));
        assert!(!is_valid_resume_confirmation("RESUME"));
    }

    #[test]
    fn testnet_confirmation_must_match_exact_phrase() {
        assert!(is_valid_testnet_order_confirmation("TESTNET ORDER"));
        assert!(!is_valid_testnet_order_confirmation("testnet order"));
        assert!(!is_valid_testnet_order_confirmation("TESTNET"));
    }

    #[test]
    fn testnet_client_order_id_is_deterministic_per_correlation_id() {
        let correlation_id =
            Uuid::parse_str("2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0").expect("valid uuid");
        assert_eq!(
            generate_testnet_client_order_id(correlation_id),
            "aegis-testnet-2ea0ed54f2bf402d8da04e92cde5b2a0"
        );
    }

    #[test]
    fn exchange_testnet_route_access_matches_role_expectations() {
        assert!(matches!(
            route_access(&axum::http::Method::GET, "/exchange/testnet/status", false),
            super::RouteAccess::Authenticated
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::GET,
                "/exchange/testnet/balances",
                false
            ),
            super::RouteAccess::Operator
        ));
        assert!(matches!(
            route_access(&axum::http::Method::POST, "/exchange/testnet/orders", false),
            super::RouteAccess::Owner
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/pipeline/preview",
                false
            ),
            super::RouteAccess::Operator
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/pipeline/submit",
                false
            ),
            super::RouteAccess::Owner
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/orders/client-1/cancel",
                false
            ),
            super::RouteAccess::Owner
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/reconcile",
                false
            ),
            super::RouteAccess::Operator
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/orders/client-1/repair",
                false
            ),
            super::RouteAccess::Operator
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::GET,
                "/exchange/testnet/orders/client-1/repairs",
                false
            ),
            super::RouteAccess::Authenticated
        ));
    }

    #[test]
    fn readiness_route_access_matches_role_expectations() {
        assert!(matches!(
            route_access(&axum::http::Method::POST, "/readiness/check", false),
            super::RouteAccess::Operator
        ));
        assert!(matches!(
            route_access(&axum::http::Method::GET, "/readiness/snapshots", false),
            super::RouteAccess::Authenticated
        ));
        assert!(matches!(
            route_access(
                &axum::http::Method::GET,
                "/readiness/snapshots/123e4567-e89b-12d3-a456-426614174000",
                false
            ),
            super::RouteAccess::Authenticated
        ));
    }

    #[test]
    fn testnet_repair_authorization_matches_role_expectations() {
        assert!(super::is_testnet_repair_authorized(
            UserRole::Operator,
            TestnetRepairAction::ManualRecheck,
            false
        ));
        assert!(super::is_testnet_repair_authorized(
            UserRole::Operator,
            TestnetRepairAction::MarkReconciliationRequired,
            false
        ));
        assert!(!super::is_testnet_repair_authorized(
            UserRole::Operator,
            TestnetRepairAction::MarkFailed,
            false
        ));
        assert!(!super::is_testnet_repair_authorized(
            UserRole::Operator,
            TestnetRepairAction::MarkCancelled,
            true
        ));
        assert!(super::is_testnet_repair_authorized(
            UserRole::Owner,
            TestnetRepairAction::SafeCancelRequest,
            true
        ));
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

    #[test]
    fn normalize_route_label_preserves_template_paths() {
        assert_eq!(normalize_route_label("/orders/:id"), "/orders/:id");
        assert_eq!(normalize_route_label("metrics"), "/metrics");
    }

    #[tokio::test]
    async fn metrics_route_returns_prometheus_text() {
        let app = Router::new()
            .route("/metrics", get(metrics))
            .with_state(test_app_state());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let text = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(text.contains("aegis_system_health_status"));
    }

    #[tokio::test]
    async fn request_middleware_uses_template_paths_for_metrics() {
        let state = test_app_state();
        let app = Router::new()
            .route("/orders/:id", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/123e4567-e89b-12d3-a456-426614174000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let encoded = telemetry().encode().expect("metrics encode");
        assert!(encoded.contains("path=\"/orders/:id\""));
        assert!(!encoded.contains("path=\"/orders/123e4567-e89b-12d3-a456-426614174000\""));
    }

    fn default_testnet_status() -> exchange::BinanceTestnetStatus {
        BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            ws_base_url: "wss://stream.testnet.binance.vision/ws".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        })
        .status()
    }

    fn auth_test_state_with_adapter(
        pool: PgPool,
        bootstrap_email: Option<&str>,
        bootstrap_password: Option<&str>,
        exchange_testnet_binance: Option<BinanceSpotTestnetAdapter>,
        exchange_testnet: Arc<dyn exchange::ExchangeAdapter>,
        exchange_testnet_status: exchange::BinanceTestnetStatus,
    ) -> AppState {
        AppState {
            config: AppConfig {
                app_name: "aegis-test-api".to_string(),
                environment: "test".to_string(),
                bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
                database_url: "postgres://unused".to_string(),
                database_max_connections: 5,
                cors_allowed_origins: parse_cors_allowed_origins(None).expect("origins"),
            },
            auth_config: AuthConfig {
                disabled: false,
                jwt_secret: Some("test-secret".to_string()),
                access_token_ttl: std::time::Duration::from_secs(900),
                refresh_token_ttl: std::time::Duration::from_secs(86_400),
                cookie_secure: false,
                protect_metrics: false,
                bootstrap_owner_email: bootstrap_email.map(|value| value.to_string()),
                bootstrap_owner_password: bootstrap_password.map(|value| value.to_string()),
            },
            db_pool: pool,
            started_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            market_mode: MarketMode::Paper,
            market_config: MarketIngestConfig {
                exchange: MarketDataSource::Binance,
                symbols: vec![Symbol::new("BTCUSDT").expect("symbol")],
                stale_threshold: std::time::Duration::from_secs(10),
                binance_ws_base_url: "wss://example.invalid".to_string(),
                binance_rest_base_url: "https://example.invalid".to_string(),
            },
            strategy_runtime: StrategyRuntimeConfig {
                default_symbols: vec![Symbol::new("BTCUSDT").expect("symbol")],
                default_timeframe: CandleInterval::OneMinute,
                default_notional: Decimal::new(100_000, 0),
                momentum_lookback_candles: 3,
                breakout_lookback_candles: 20,
            },
            exchange_testnet_binance,
            exchange_testnet,
            exchange_testnet_environment: ExchangeEnvironment::Testnet,
            exchange_testnet_status,
        }
    }

    fn auth_test_state(
        pool: PgPool,
        bootstrap_email: Option<&str>,
        bootstrap_password: Option<&str>,
    ) -> AppState {
        let adapter = BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            ws_base_url: "wss://stream.testnet.binance.vision/ws".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        });
        let status = adapter.status();
        auth_test_state_with_adapter(
            pool,
            bootstrap_email,
            bootstrap_password,
            Some(adapter.clone()),
            Arc::new(adapter.clone()),
            status,
        )
    }

    fn auth_test_router(state: AppState) -> Router {
        Router::new()
            .route("/auth/bootstrap-owner", post(bootstrap_owner))
            .route("/auth/login", post(login))
            .route("/auth/refresh", post(refresh))
            .route("/auth/logout", post(logout))
            .route(
                "/strategy/:id/enable",
                post(|| async { (StatusCode::OK, Json(json!({ "ok": true }))) }),
            )
            .route(
                "/risk/resume",
                post(|| async { (StatusCode::OK, Json(json!({ "ok": true }))) }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn repair_test_router(state: AppState) -> Router {
        Router::new()
            .route("/auth/login", post(login))
            .route(
                "/exchange/testnet/orders/:client_order_id/cancel",
                post(cancel_exchange_testnet_order),
            )
            .route(
                "/exchange/testnet/orders/:client_order_id/repair",
                post(repair_exchange_testnet_order),
            )
            .route(
                "/exchange/testnet/orders/:client_order_id/repairs",
                get(list_exchange_testnet_order_repairs),
            )
            .route(
                "/exchange/testnet/reconcile",
                post(reconcile_exchange_testnet_orders_handler),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn pipeline_test_router(state: AppState) -> Router {
        Router::new()
            .route("/auth/login", post(login))
            .route(
                "/exchange/testnet/pipeline/preview",
                post(preview_exchange_testnet_pipeline),
            )
            .route(
                "/exchange/testnet/pipeline/submit",
                post(submit_exchange_testnet_pipeline),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn shadow_test_router(state: AppState) -> Router {
        Router::new()
            .route("/auth/login", post(login))
            .route(
                "/exchange/testnet/shadow/run",
                post(run_exchange_testnet_shadow_handler),
            )
            .route(
                "/exchange/testnet/shadow/runs",
                get(list_exchange_testnet_shadow_runs_handler),
            )
            .route(
                "/exchange/testnet/shadow/runs/:id",
                get(get_exchange_testnet_shadow_run_handler),
            )
            .route(
                "/exchange/testnet/shadow/promotions/preview",
                post(preview_exchange_testnet_shadow_promotion_handler),
            )
            .route(
                "/exchange/testnet/shadow/promotions",
                get(list_exchange_testnet_shadow_promotions_handler),
            )
            .route(
                "/exchange/testnet/shadow/promotions/:id",
                get(get_exchange_testnet_shadow_promotion_handler),
            )
            .route(
                "/exchange/testnet/shadow/promotions/:id/submit",
                post(submit_exchange_testnet_shadow_promotion_handler),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn risk_test_router(state: AppState) -> Router {
        Router::new()
            .route("/risk/decisions", get(get_risk_decisions))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn strategy_test_router(state: AppState) -> Router {
        Router::new()
            .route("/strategy/list", get(get_strategy_list))
            .route(
                "/strategy/:id/diagnostics",
                get(strategy_diagnostics_handler),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    fn readiness_test_router(state: AppState) -> Router {
        Router::new()
            .route("/auth/login", post(login))
            .route("/readiness/check", post(check_execution_readiness_handler))
            .route(
                "/readiness/snapshots",
                get(list_execution_readiness_snapshots_handler),
            )
            .route(
                "/readiness/snapshots/:id",
                get(get_execution_readiness_snapshot_handler),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                request_context_middleware,
            ))
            .with_state(state)
    }

    async fn response_json<T: DeserializeOwned>(response: axum::response::Response) -> T {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&body).expect("json body")
    }

    fn cli_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header(CLI_AUTH_MODE_HEADER, CLI_AUTH_MODE_VALUE)
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    fn bearer_request(method: &str, uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request")
    }

    async fn setup_optional_test_db() -> Option<TestDatabase> {
        match TestDatabase::setup().await {
            Ok(test_db) => Some(test_db),
            Err(err)
                if err
                    .to_string()
                    .contains("refusing to run integration tests against database")
                    || err.to_string().contains(
                        "set TEST_DATABASE_URL or DATABASE_URL before running DB integration tests",
                    ) =>
            {
                None
            }
            Err(err) => panic!("test db: {err}"),
        }
    }

    fn extract_set_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
        headers
            .get(SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
    }

    async fn login_cli(app: &Router, email: &str, password: &str) -> (AuthLoginResponse, String) {
        let response = app
            .clone()
            .oneshot(cli_request(
                "POST",
                "/auth/login",
                json!({ "email": email, "password": password }),
            ))
            .await
            .expect("login response");
        let cookie = extract_set_cookie(response.headers()).expect("refresh cookie");
        let payload = response_json::<AuthLoginResponse>(response).await;
        (payload, cookie)
    }

    async fn insert_test_user(pool: &PgPool, email: &str, password: &str, role: UserRole) {
        let password_hash = hash_password(password).expect("password hash");
        insert_user(
            pool,
            Uuid::new_v4(),
            email,
            &password_hash,
            role,
            UserStatus::Active,
        )
        .await
        .expect("user insert");
    }

    fn sample_testnet_order(
        client_order_id: &str,
        status: &str,
        execution_state: TestnetExecutionState,
        symbol: &str,
    ) -> ExchangeTestnetOrderRecord {
        let timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        ExchangeTestnetOrderRecord {
            id: Uuid::new_v4(),
            exchange: "binance".to_string(),
            environment: "testnet".to_string(),
            client_order_id: client_order_id.to_string(),
            exchange_order_id: Some(format!("ex-{client_order_id}")),
            symbol: symbol.to_string(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: Some("GTC".to_string()),
            requested_qty: Some(Decimal::ONE),
            requested_notional: None,
            limit_price: Some(Decimal::new(100_000, 0)),
            status: status.to_string(),
            execution_state: execution_state.as_str().to_string(),
            ack_payload: Some(json!({ "status": status })),
            latest_status_payload: Some(json!({ "status": status })),
            risk_decision_id: None,
            created_by: None,
            last_transition_at: Some(timestamp),
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    fn sample_market_tick(symbol: &str, price: Decimal) -> MarketTick {
        MarketTick {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new(symbol).expect("symbol"),
            price,
            quantity: Decimal::ONE,
            trade_time: Utc::now(),
            received_at: Utc::now(),
            raw_payload: None,
        }
    }

    async fn insert_test_risk_decision(
        pool: &PgPool,
        decision: &str,
        symbol: &str,
        side: &str,
        approved_notional: Decimal,
    ) -> Uuid {
        let id = Uuid::new_v4();
        let rationale = json!({
            "strategy_id": "momentum_v1",
            "symbol": symbol,
            "side": side,
            "suggested_notional": approved_notional.to_string(),
            "approved_notional": approved_notional.to_string(),
            "risk_score": "1",
            "reasons": [],
            "rule_results": [],
        });
        sqlx::query(
            r#"
            INSERT INTO risk_decisions (id, correlation_id, signal_id, decision, rationale, decided_at)
            VALUES ($1, $2, NULL, $3, $4, $5)
            "#,
        )
        .bind(id)
        .bind(Uuid::new_v4())
        .bind(decision)
        .bind(rationale.to_string())
        .bind(Utc::now())
        .execute(pool)
        .await
        .expect("risk decision insert");
        id
    }

    async fn latest_audit_log_for_target(
        pool: &PgPool,
        action: &str,
        target: &str,
    ) -> (String, Value) {
        let row = sqlx::query(
            r#"
            SELECT actor, metadata
            FROM audit_logs
            WHERE action = $1 AND target = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(action)
        .bind(target)
        .fetch_one(pool)
        .await
        .expect("audit log query should succeed");

        (row.get("actor"), row.get("metadata"))
    }

    async fn system_events_for_order(
        pool: &PgPool,
        client_order_id: &str,
        event_type: &str,
    ) -> Vec<Value> {
        sqlx::query(
            r#"
            SELECT payload
            FROM system_events
            WHERE event_type = $1
              AND payload ->> 'client_order_id' = $2
            ORDER BY created_at ASC
            "#,
        )
        .bind(event_type)
        .bind(client_order_id)
        .fetch_all(pool)
        .await
        .expect("system events query should succeed")
        .into_iter()
        .map(|row| row.get("payload"))
        .collect()
    }

    async fn count_system_events(pool: &PgPool, event_type: &str) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM system_events WHERE event_type = $1")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .expect("system event count")
            .get::<i64, _>("count")
    }

    async fn count_audit_logs(pool: &PgPool, action: &str) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM audit_logs WHERE action = $1")
            .bind(action)
            .fetch_one(pool)
            .await
            .expect("audit log count")
            .get::<i64, _>("count")
    }

    async fn count_exchange_testnet_lifecycle_events(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM exchange_testnet_order_lifecycle_events")
            .fetch_one(pool)
            .await
            .expect("lifecycle event count")
            .get::<i64, _>("count")
    }

    async fn count_execution_readiness_snapshots(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM execution_readiness_snapshots")
            .fetch_one(pool)
            .await
            .expect("readiness snapshot count")
            .get::<i64, _>("count")
    }

    async fn count_exchange_testnet_orders(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM exchange_testnet_orders")
            .fetch_one(pool)
            .await
            .expect("testnet order count")
            .get::<i64, _>("count")
    }

    async fn count_backtest_runs(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM backtest_runs")
            .fetch_one(pool)
            .await
            .expect("backtest run count")
            .get::<i64, _>("count")
    }

    async fn count_backtest_trades(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM backtest_trades")
            .fetch_one(pool)
            .await
            .expect("backtest trade count")
            .get::<i64, _>("count")
    }

    async fn count_paper_fills(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM paper_fills")
            .fetch_one(pool)
            .await
            .expect("paper fill count")
            .get::<i64, _>("count")
    }

    async fn count_paper_positions(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM paper_positions")
            .fetch_one(pool)
            .await
            .expect("paper position count")
            .get::<i64, _>("count")
    }

    async fn count_paper_orders(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM orders")
            .fetch_one(pool)
            .await
            .expect("paper order count")
            .get::<i64, _>("count")
    }

    async fn count_signals(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM signals")
            .fetch_one(pool)
            .await
            .expect("signal count")
            .get::<i64, _>("count")
    }

    async fn count_risk_decisions(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM risk_decisions")
            .fetch_one(pool)
            .await
            .expect("risk decision count")
            .get::<i64, _>("count")
    }

    async fn count_paper_equity_snapshots(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM paper_equity_snapshots")
            .fetch_one(pool)
            .await
            .expect("paper equity snapshot count")
            .get::<i64, _>("count")
    }

    async fn count_paper_trade_journal_rows(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM paper_trade_journal")
            .fetch_one(pool)
            .await
            .expect("paper trade journal count")
            .get::<i64, _>("count")
    }

    async fn assert_no_paper_or_backtest_mutation(pool: &PgPool) {
        assert!(list_orders(pool).await.expect("paper orders").is_empty());
        assert_eq!(count_paper_positions(pool).await, 0);
        assert_eq!(count_paper_fills(pool).await, 0);
        assert_eq!(count_paper_equity_snapshots(pool).await, 0);
        assert_eq!(count_paper_trade_journal_rows(pool).await, 0);
        assert_eq!(count_backtest_runs(pool).await, 0);
        assert_eq!(count_backtest_trades(pool).await, 0);
    }

    async fn count_system_events_for_target(pool: &PgPool, event_type: &str, target: &str) -> i64 {
        sqlx::query(
            "SELECT COUNT(*) AS count FROM system_events WHERE event_type = $1 AND payload ->> 'symbol' = $2",
        )
        .bind(event_type)
        .bind(target)
        .fetch_one(pool)
        .await
            .expect("system event count for target")
            .get::<i64, _>("count")
    }

    #[tokio::test]
    async fn risk_decisions_returns_empty_array_when_no_rows_exist() {
        let Some(test_db) = setup_optional_test_db().await else {
            return;
        };
        let app = risk_test_router(auth_test_state(test_db.pool.clone(), None, None));

        let response = app
            .oneshot(get_request("/risk/decisions"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let payload = response_json::<Value>(response).await;
        assert_eq!(
            payload["decisions"]
                .as_array()
                .expect("decisions array")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn risk_decisions_returns_recent_decisions_without_symbol_filter() {
        let Some(test_db) = setup_optional_test_db().await else {
            return;
        };
        let app = risk_test_router(auth_test_state(test_db.pool.clone(), None, None));
        let btc_decision = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "BUY",
            Decimal::new(100_000, 0),
        )
        .await;
        let eth_decision = insert_test_risk_decision(
            &test_db.pool,
            "RISK_REJECTED",
            "ETHUSDT",
            "SELL",
            Decimal::new(50_000, 0),
        )
        .await;

        let response = app
            .oneshot(get_request("/risk/decisions"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let payload = response_json::<Value>(response).await;
        let decisions = payload["decisions"].as_array().expect("decisions array");
        let ids = decisions
            .iter()
            .filter_map(|decision| decision["id"].as_str())
            .collect::<Vec<_>>();
        let symbols = decisions
            .iter()
            .filter_map(|decision| decision["symbol"].as_str())
            .collect::<Vec<_>>();
        let btc_id = btc_decision.to_string();
        let eth_id = eth_decision.to_string();

        assert_eq!(decisions.len(), 2);
        assert!(ids.contains(&btc_id.as_str()));
        assert!(ids.contains(&eth_id.as_str()));
        assert!(symbols.contains(&"BTCUSDT"));
        assert!(symbols.contains(&"ETHUSDT"));
    }

    #[tokio::test]
    async fn risk_decisions_applies_symbol_filter() {
        let Some(test_db) = setup_optional_test_db().await else {
            return;
        };
        let app = risk_test_router(auth_test_state(test_db.pool.clone(), None, None));
        let btc_decision = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "BUY",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "ETHUSDT",
            "BUY",
            Decimal::new(100_000, 0),
        )
        .await;

        let response = app
            .oneshot(get_request("/risk/decisions?symbol=BTCUSDT"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let payload = response_json::<Value>(response).await;
        let decisions = payload["decisions"].as_array().expect("decisions array");
        let btc_id = btc_decision.to_string();

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0]["id"].as_str(), Some(btc_id.as_str()));
        assert_eq!(decisions[0]["symbol"].as_str(), Some("BTCUSDT"));
    }

    #[tokio::test]
    async fn strategy_list_bootstraps_configs_with_status_and_is_idempotent() {
        let Some(test_db) = setup_optional_test_db().await else {
            return;
        };
        let app = strategy_test_router(auth_test_state(test_db.pool.clone(), None, None));

        let first_response = app
            .clone()
            .oneshot(get_request("/strategy/list"))
            .await
            .expect("first response");
        assert_eq!(first_response.status(), StatusCode::OK);

        let first_payload = response_json::<Value>(first_response).await;
        let strategies = first_payload["strategies"]
            .as_array()
            .expect("strategies array");
        assert_eq!(strategies.len(), 2);
        assert!(strategies
            .iter()
            .all(|strategy| strategy["enabled"].as_bool() == Some(true)));

        let rows = sqlx::query(
            r#"
            SELECT strategy_id, status
            FROM strategy_configs
            ORDER BY strategy_id
            "#,
        )
        .fetch_all(&test_db.pool)
        .await
        .expect("strategy config rows");
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row.get::<String, _>("status") == "enabled"));

        let second_response = app
            .oneshot(get_request("/strategy/list"))
            .await
            .expect("second response");
        assert_eq!(second_response.status(), StatusCode::OK);

        let config_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM strategy_configs")
            .fetch_one(&test_db.pool)
            .await
            .expect("strategy config count");
        assert_eq!(config_count, 2);
    }

    #[tokio::test]
    async fn strategy_diagnostics_reads_closed_candles_without_mutating_state() {
        let Some(test_db) = setup_optional_test_db().await else {
            return;
        };
        let app = strategy_test_router(auth_test_state(test_db.pool.clone(), None, None));
        seed_shadow_candles(&test_db.pool, "BTCUSDT", &[100_000, 99_900, 99_800, 99_700]).await;

        let before_signals = count_signals(&test_db.pool).await;
        let before_risk_decisions = count_risk_decisions(&test_db.pool).await;
        let before_orders = count_paper_orders(&test_db.pool).await;

        let response = app
            .oneshot(get_request(
                "/strategy/momentum_v1/diagnostics?symbol=BTCUSDT&timeframe=1m&limit=20",
            ))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let payload = response_json::<Value>(response).await;
        let decision = payload["result"]["final_decision"]
            .as_str()
            .expect("final decision");
        assert!(matches!(decision, "NO_SIGNAL" | "WOULD_SIGNAL"));
        assert_eq!(
            payload["result"]["data_health"]["available_closed_candles"].as_i64(),
            Some(4)
        );
        assert_eq!(count_signals(&test_db.pool).await, before_signals);
        assert_eq!(
            count_risk_decisions(&test_db.pool).await,
            before_risk_decisions
        );
        assert_eq!(count_paper_orders(&test_db.pool).await, before_orders);
    }

    async fn insert_recent_closed_candle(pool: &PgPool, symbol: &str, close: Decimal) {
        let now = Utc::now();
        let candle = Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new(symbol).expect("symbol"),
            interval: CandleInterval::OneMinute,
            open_time: now - chrono::Duration::minutes(1),
            close_time: now - chrono::Duration::seconds(1),
            open: close,
            high: close,
            low: close,
            close,
            volume: Decimal::ONE,
            quote_volume: Some(close),
            trade_count: 1,
            is_closed: true,
            created_at: now,
            updated_at: now,
        };
        upsert_candle(pool, &candle)
            .await
            .expect("recent candle should persist");
    }

    fn readiness_risk_config() -> RiskConfig {
        RiskConfig {
            max_open_positions: 2,
            max_daily_loss_pct: Decimal::new(2, 0),
            max_weekly_loss_pct: Decimal::new(5, 0),
            max_position_notional: Decimal::new(500_000, 0),
            max_slippage_pct: Decimal::new(1, 0),
            max_consecutive_losses: 3,
            cooldown_seconds: 900,
            max_signal_age_ms: 5_000,
            stale_feed_threshold_seconds: 10,
        }
    }

    async fn insert_active_paper_account(pool: &PgPool) {
        insert_paper_account(
            pool,
            &PaperAccount {
                id: Uuid::new_v4(),
                name: "Default Paper".to_string(),
                base_currency: "USDT".to_string(),
                initial_equity: Decimal::new(1_000_000, 0),
                current_equity: Decimal::new(1_000_000, 0),
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                status: PaperAccountStatus::Active,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        )
        .await
        .expect("paper account");
    }

    async fn seed_readiness_market(pool: &PgPool, symbol: &str) {
        seed_shadow_feed(pool, symbol).await;
        seed_shadow_candles(pool, symbol, &[100_000, 101_000, 102_000, 103_000]).await;
        insert_market_tick(pool, &sample_market_tick(symbol, Decimal::new(103_000, 0)))
            .await
            .expect("market tick");
    }

    async fn assert_readiness_read_only(
        pool: &PgPool,
        expected_snapshot_count: i64,
        before: (i64, i64, i64, i64, i64),
    ) {
        assert_eq!(count_paper_orders(pool).await, before.0);
        assert_eq!(count_exchange_testnet_orders(pool).await, before.1);
        assert_eq!(
            count_exchange_testnet_lifecycle_events(pool).await,
            before.2
        );
        assert_eq!(count_backtest_runs(pool).await, before.3);
        assert_eq!(count_backtest_trades(pool).await, before.4);
        assert_eq!(
            count_execution_readiness_snapshots(pool).await,
            expected_snapshot_count
        );
    }

    async fn readiness_mutation_counts(pool: &PgPool) -> (i64, i64, i64, i64, i64) {
        (
            count_paper_orders(pool).await,
            count_exchange_testnet_orders(pool).await,
            count_exchange_testnet_lifecycle_events(pool).await,
            count_backtest_runs(pool).await,
            count_backtest_trades(pool).await,
        )
    }

    fn sample_shadow_run_would_submit(created_at: chrono::DateTime<Utc>) -> TestnetShadowRunRecord {
        TestnetShadowRunRecord {
            id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            decision: "WOULD_SUBMIT".to_string(),
            signal_id: None,
            risk_decision_id: None,
            would_submit_payload: Some(json!({"symbol":"BTCUSDT"})),
            price_source: Some("local_tick".to_string()),
            resolved_price: Some(Decimal::new(103_000, 0)),
            reasons: vec!["shadow coverage".to_string()],
            status: "COMPLETED".to_string(),
            created_at,
            correlation_id: Some(Uuid::new_v4()),
        }
    }

    fn shadow_strategy_config(enabled: bool) -> StrategyConfig {
        StrategyConfig {
            strategy_id: StrategyId::MomentumV1,
            enabled,
            mode: StrategyMode::Shadow,
            symbols: vec![Symbol::new("BTCUSDT").expect("symbol")],
            timeframe: CandleInterval::OneMinute,
            suggested_notional: Decimal::new(100_000, 0),
            max_signal_age_ms: 180_000,
            cooldown_seconds: 900,
            lookback_candles: 3,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("shadow test".to_string()),
        }
    }

    async fn seed_shadow_feed(pool: &PgPool, symbol: &str) {
        upsert_market_feed_status(
            pool,
            MarketDataSource::Binance,
            &Symbol::new(symbol).expect("symbol"),
            FeedStatus::Connected,
            DataFreshnessStatus::Fresh,
            Some(Utc::now()),
            None,
            0,
        )
        .await
        .expect("feed status");
    }

    async fn seed_shadow_candles(pool: &PgPool, symbol: &str, closes: &[i64]) {
        let base_open = Utc::now() - chrono::Duration::minutes(closes.len() as i64 + 1);
        for (index, close) in closes.iter().enumerate() {
            let open_time = base_open + chrono::Duration::minutes(index as i64);
            let candle = Candle {
                id: Uuid::new_v4(),
                exchange: MarketDataSource::Binance,
                symbol: Symbol::new(symbol).expect("symbol"),
                interval: CandleInterval::OneMinute,
                open_time,
                close_time: open_time + chrono::Duration::minutes(1),
                open: Decimal::new(*close - 100, 0),
                high: Decimal::new(*close + 100, 0),
                low: Decimal::new(*close - 200, 0),
                close: Decimal::new(*close, 0),
                volume: Decimal::ONE,
                quote_volume: Some(Decimal::new(*close, 0)),
                trade_count: 1,
                is_closed: true,
                created_at: open_time + chrono::Duration::seconds(59),
                updated_at: open_time + chrono::Duration::seconds(59),
            };
            upsert_candle(pool, &candle).await.expect("shadow candle");
        }
    }

    async fn count_testnet_shadow_runs(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM testnet_shadow_runs")
            .fetch_one(pool)
            .await
            .expect("shadow run count")
            .get::<i64, _>("count")
    }

    async fn count_testnet_shadow_promotions(pool: &PgPool) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM testnet_shadow_promotions")
            .fetch_one(pool)
            .await
            .expect("shadow promotion count")
            .get::<i64, _>("count")
    }

    async fn setup_shadow_promotion_fixture(pool: &PgPool) {
        upsert_strategy_config(pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        seed_shadow_feed(pool, "BTCUSDT").await;
        seed_shadow_candles(pool, "BTCUSDT", &[100_000, 101_000, 102_000, 103_000]).await;
        insert_market_tick(
            pool,
            &sample_market_tick("BTCUSDT", Decimal::new(103_000, 0)),
        )
        .await
        .expect("market tick");
    }

    async fn create_previewable_shadow_run(
        app: &Router,
        operator_token: &str,
    ) -> TestnetShadowRunResponse {
        let shadow_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                operator_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("shadow response");
        assert_eq!(shadow_response.status(), StatusCode::OK);
        let shadow = response_json::<TestnetShadowRunResponse>(shadow_response).await;
        assert_eq!(shadow.run.decision, TestnetShadowDecision::WouldSubmit);
        shadow
    }

    async fn create_shadow_promotion_preview(
        app: &Router,
        operator_token: &str,
        shadow_run_id: Uuid,
    ) -> TestnetShadowPromotionResponse {
        let preview_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/promotions/preview",
                operator_token,
                json!({ "shadow_run_id": shadow_run_id }),
            ))
            .await
            .expect("preview response");
        assert_eq!(preview_response.status(), StatusCode::OK);
        response_json::<TestnetShadowPromotionResponse>(preview_response).await
    }

    async fn age_shadow_reference_price(pool: &PgPool, symbol: &str, age_seconds: i64) {
        let stale_at = Utc::now() - chrono::Duration::seconds(age_seconds);
        sqlx::query(
            r#"
            UPDATE market_ticks
            SET received_at = $1, trade_time = $1
            WHERE symbol = $2
            "#,
        )
        .bind(stale_at)
        .bind(symbol)
        .execute(pool)
        .await
        .expect("market tick should age");
        sqlx::query(
            r#"
            UPDATE candles
            SET close_time = $1, updated_at = $1, created_at = $1
            WHERE symbol = $2
            "#,
        )
        .bind(stale_at)
        .bind(symbol)
        .execute(pool)
        .await
        .expect("candles should age");
    }

    async fn shadow_promotion_record(
        pool: &PgPool,
        promotion_id: Uuid,
    ) -> db::TestnetShadowPromotionRecord {
        db::get_testnet_shadow_promotion_by_id(pool, promotion_id)
            .await
            .expect("shadow promotion query")
            .expect("shadow promotion should exist")
    }

    #[test]
    fn exchange_testnet_shadow_promotion_submit_route_requires_owner() {
        assert!(matches!(
            route_access(
                &axum::http::Method::POST,
                "/exchange/testnet/shadow/promotions/123/submit",
                false
            ),
            super::RouteAccess::Owner
        ));
    }

    #[test]
    fn shadow_promotion_submit_confirmation_must_match_exact_phrase() {
        let request = TestnetShadowPromotionSubmitRequest {
            confirmation_text: expected_testnet_shadow_promotion_confirmation("BTCUSDT"),
            correlation_id: None,
        };
        assert!(request.validate_confirmation("BTCUSDT").is_ok());

        let invalid = TestnetShadowPromotionSubmitRequest {
            confirmation_text: "promote testnet btcusdt".to_string(),
            correlation_id: None,
        };
        assert!(invalid.validate_confirmation("BTCUSDT").is_err());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_preview_does_not_submit_orders() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(100_000, 0)),
        )
        .await
        .expect("market tick");

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/preview",
                &owner_login.access_token,
                json!({ "risk_decision_id": risk_decision_id }),
            ))
            .await
            .expect("preview response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<ExchangeTestnetPipelinePreviewResponse>(response).await;
        assert_eq!(payload.preview.risk_decision_id, risk_decision_id);
        assert_eq!(
            payload.preview.confirmation_text,
            expected_testnet_pipeline_confirmation("BTCUSDT")
        );
        assert_eq!(
            count_audit_logs(&test_db.pool, "exchange.testnet.pipeline.previewed").await,
            1
        );
        assert_eq!(
            count_system_events_for_target(
                &test_db.pool,
                "exchange.testnet.pipeline.previewed",
                "BTCUSDT",
            )
            .await,
            1
        );
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert!(list_orders(&test_db.pool)
            .await
            .expect("list paper orders")
            .is_empty());
        assert_eq!(count_backtest_runs(&test_db.pool).await, 0);
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_submit_requires_owner_and_confirmation() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(100_000, 0)),
        )
        .await
        .expect("market tick");

        let operator_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &operator_login.access_token,
                json!({
                    "risk_decision_id": risk_decision_id,
                    "confirmation_text": expected_testnet_pipeline_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("operator submit response");
        assert_eq!(operator_response.status(), StatusCode::FORBIDDEN);

        let wrong_confirmation = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &owner_login.access_token,
                json!({
                    "risk_decision_id": risk_decision_id,
                    "confirmation_text": "SUBMIT TESTNET ETHUSDT"
                }),
            ))
            .await
            .expect("owner submit response");
        assert_eq!(wrong_confirmation.status(), StatusCode::CONFLICT);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_kill_switch_and_rejected_risk_block_preview_and_submit() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(100_000, 0)),
        )
        .await
        .expect("market tick");
        let approved_risk = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        let rejected_risk = insert_test_risk_decision(
            &test_db.pool,
            "REJECTED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;

        set_kill_switch_state(
            &test_db.pool,
            &StateActor::system("test"),
            Uuid::new_v4(),
            "testnet_pipeline",
            true,
            Some("manual test block".to_string()),
        )
        .await
        .expect("kill switch update");

        let kill_switch_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/preview",
                &owner_login.access_token,
                json!({ "risk_decision_id": approved_risk }),
            ))
            .await
            .expect("kill switch preview");
        assert_eq!(kill_switch_response.status(), StatusCode::CONFLICT);
        let kill_switch_submit_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &owner_login.access_token,
                json!({
                    "risk_decision_id": approved_risk,
                    "confirmation_text": expected_testnet_pipeline_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("kill switch submit");
        assert_eq!(kill_switch_submit_response.status(), StatusCode::CONFLICT);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());

        set_kill_switch_state(
            &test_db.pool,
            &StateActor::system("test"),
            Uuid::new_v4(),
            "testnet_pipeline",
            false,
            Some("manual test resume".to_string()),
        )
        .await
        .expect("kill switch reset");

        let rejected_risk_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/preview",
                &owner_login.access_token,
                json!({ "risk_decision_id": rejected_risk }),
            ))
            .await
            .expect("rejected risk preview");
        assert_eq!(rejected_risk_response.status(), StatusCode::CONFLICT);
        let rejected_risk_submit_response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &owner_login.access_token,
                json!({
                    "risk_decision_id": rejected_risk,
                    "confirmation_text": expected_testnet_pipeline_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("rejected risk submit");
        assert_eq!(rejected_risk_submit_response.status(), StatusCode::CONFLICT);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_submit_happy_path_uses_fake_adapter_and_stays_isolated() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_ack(FakeSubmitAck {
            exchange_order_id: Some("fake-submit-ack-1".to_string()),
            ..FakeSubmitAck::default()
        });
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange),
            FakeExchangeAdapter::status(),
        );
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(100_000, 0)),
        )
        .await
        .expect("market tick");

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &owner_login.access_token,
                json!({
                    "risk_decision_id": risk_decision_id,
                    "confirmation_text": expected_testnet_pipeline_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let orders = list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders");
        assert_eq!(orders.len(), 1);
        let order = &orders[0];
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.risk_decision_id, Some(risk_decision_id));
        assert_eq!(order.execution_state, "EXCHANGE_ACKED");
        assert_eq!(
            order.exchange_order_id.as_deref(),
            Some("fake-submit-ack-1")
        );
        let lifecycle =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
                .await
                .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(lifecycle[0].next_state, "ORDER_SUBMIT_REQUESTED");
        assert_eq!(lifecycle[1].next_state, "EXCHANGE_ACKED");
        assert_eq!(
            count_system_events_for_target(
                &test_db.pool,
                "exchange.testnet.pipeline.submit_requested",
                "BTCUSDT",
            )
            .await,
            1
        );
        assert_eq!(
            system_events_for_order(
                &test_db.pool,
                &order.client_order_id,
                "exchange.testnet.order.acked"
            )
            .await
            .len(),
            1
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_submit_adapter_failure_persists_request_without_acking() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_error(aegis_core::ExchangeError::Transport(
            "deterministic timeout".to_string(),
        ));
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange),
            FakeExchangeAdapter::status(),
        );
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(100_000, 0)),
        )
        .await
        .expect("market tick");

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/submit",
                &owner_login.access_token,
                json!({
                    "risk_decision_id": risk_decision_id,
                    "confirmation_text": expected_testnet_pipeline_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let order = list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .into_iter()
            .next()
            .expect("order should exist");
        assert_eq!(order.execution_state, "ORDER_SUBMIT_REQUESTED");
        let lifecycle =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
                .await
                .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].next_state, "ORDER_SUBMIT_REQUESTED");
        assert!(system_events_for_order(
            &test_db.pool,
            &order.client_order_id,
            "exchange.testnet.order.acked"
        )
        .await
        .is_empty());
        let (_, metadata) = latest_audit_log_for_target(
            &test_db.pool,
            "exchange.testnet.order.rejected",
            "BTCUSDT",
        )
        .await;
        assert_eq!(
            metadata.get("error").and_then(Value::as_str),
            Some("exchange_testnet_request_rejected")
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_run_persists_no_signal() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        seed_shadow_feed(&test_db.pool, "BTCUSDT").await;
        seed_shadow_candles(&test_db.pool, "BTCUSDT", &[100_000, 99_900, 99_800, 99_700]).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                &operator_login.access_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("shadow response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<TestnetShadowRunResponse>(response).await;
        assert_eq!(payload.run.decision, TestnetShadowDecision::NoSignal);
        assert_eq!(count_testnet_shadow_runs(&test_db.pool).await, 1);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_run_persists_risk_rejected() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        upsert_risk_config(
            &test_db.pool,
            &RiskConfig {
                max_open_positions: 2,
                max_daily_loss_pct: Decimal::new(2, 0),
                max_weekly_loss_pct: Decimal::new(5, 0),
                max_position_notional: Decimal::new(1, 0),
                max_slippage_pct: Decimal::new(1, 0),
                max_consecutive_losses: 3,
                cooldown_seconds: 900,
                max_signal_age_ms: 5_000,
                stale_feed_threshold_seconds: 10,
            },
        )
        .await
        .expect("risk config");
        seed_shadow_feed(&test_db.pool, "BTCUSDT").await;
        seed_shadow_candles(
            &test_db.pool,
            "BTCUSDT",
            &[100_000, 101_000, 102_000, 103_000],
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(103_000, 0)),
        )
        .await
        .expect("market tick");
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                &operator_login.access_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("shadow response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<TestnetShadowRunResponse>(response).await;
        assert_eq!(payload.run.decision, TestnetShadowDecision::RiskRejected);
        assert!(payload.run.risk_decision_id.is_some());
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_run_persists_would_submit_without_testnet_order_or_lifecycle() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        seed_shadow_feed(&test_db.pool, "BTCUSDT").await;
        seed_shadow_candles(
            &test_db.pool,
            "BTCUSDT",
            &[100_000, 101_000, 102_000, 103_000],
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(103_000, 0)),
        )
        .await
        .expect("market tick");
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                &operator_login.access_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("shadow response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<TestnetShadowRunResponse>(response).await;
        assert_eq!(payload.run.decision, TestnetShadowDecision::WouldSubmit);
        assert!(payload.run.would_submit_order.is_some());
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_run_listing_is_isolated_and_ordered() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Viewer,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        seed_shadow_feed(&test_db.pool, "BTCUSDT").await;
        seed_shadow_candles(
            &test_db.pool,
            "BTCUSDT",
            &[100_000, 101_000, 102_000, 103_000],
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(103_000, 0)),
        )
        .await
        .expect("market tick");
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (viewer_login, _) = login_cli(
            &app,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(bearer_request(
                    "POST",
                    "/exchange/testnet/shadow/run",
                    &operator_login.access_token,
                    json!({
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m"
                    }),
                ))
                .await
                .expect("shadow response");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let response = app
            .clone()
            .oneshot(bearer_request(
                "GET",
                "/exchange/testnet/shadow/runs?limit=10",
                &viewer_login.access_token,
                json!({}),
            ))
            .await
            .expect("shadow list response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<TestnetShadowRunsResponse>(response).await;
        assert!(payload.runs.len() >= 2);
        assert!(payload.runs[0].created_at >= payload.runs[1].created_at);
        assert_eq!(count_testnet_shadow_runs(&test_db.pool).await, 2);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_preview_persists_without_testnet_order_or_lifecycle() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;
        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;

        assert_eq!(preview.promotion.shadow_run_id, shadow.run.run_id);
        assert_eq!(preview.promotion.status.as_str(), "PREVIEWED");
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Previewed.as_str()
        );
        assert!(persisted.expires_at > persisted.created_at);
        assert_eq!(persisted.shadow_run_id, shadow.run.run_id);
        assert_eq!(persisted.risk_decision_id, shadow.run.risk_decision_id);
        assert_eq!(count_testnet_shadow_promotions(&test_db.pool).await, 1);
        assert_eq!(
            count_audit_logs(&test_db.pool, "exchange.testnet.shadow_promotion.previewed").await,
            1
        );
        assert_eq!(
            count_system_events(&test_db.pool, "exchange.testnet.shadow_promotion.previewed").await,
            1
        );
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_preview_rejects_non_would_submit_runs_without_persisting() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        seed_shadow_feed(&test_db.pool, "BTCUSDT").await;
        seed_shadow_candles(&test_db.pool, "BTCUSDT", &[100_000, 99_900, 99_800, 99_700]).await;

        let no_signal_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                &operator_login.access_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("no signal shadow response");
        let no_signal = response_json::<TestnetShadowRunResponse>(no_signal_response).await;
        assert_eq!(no_signal.run.decision, TestnetShadowDecision::NoSignal);

        let no_signal_preview = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/promotions/preview",
                &operator_login.access_token,
                json!({ "shadow_run_id": no_signal.run.run_id }),
            ))
            .await
            .expect("no signal preview response");
        assert_eq!(no_signal_preview.status(), StatusCode::CONFLICT);
        let no_signal_error = response_json::<Value>(no_signal_preview).await;
        assert_eq!(no_signal_error["error"], "shadow_decision_not_would_submit");

        upsert_risk_config(
            &test_db.pool,
            &RiskConfig {
                max_open_positions: 2,
                max_daily_loss_pct: Decimal::new(2, 0),
                max_weekly_loss_pct: Decimal::new(5, 0),
                max_position_notional: Decimal::new(1, 0),
                max_slippage_pct: Decimal::new(1, 0),
                max_consecutive_losses: 3,
                cooldown_seconds: 900,
                max_signal_age_ms: 5_000,
                stale_feed_threshold_seconds: 10,
            },
        )
        .await
        .expect("risk config");
        seed_shadow_candles(
            &test_db.pool,
            "BTCUSDT",
            &[100_000, 101_000, 102_000, 103_000],
        )
        .await;
        insert_market_tick(
            &test_db.pool,
            &sample_market_tick("BTCUSDT", Decimal::new(103_000, 0)),
        )
        .await
        .expect("market tick");

        let risk_rejected_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/run",
                &operator_login.access_token,
                json!({
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("risk rejected shadow response");
        let risk_rejected = response_json::<TestnetShadowRunResponse>(risk_rejected_response).await;
        assert_eq!(
            risk_rejected.run.decision,
            TestnetShadowDecision::RiskRejected
        );

        let risk_rejected_preview = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/promotions/preview",
                &operator_login.access_token,
                json!({ "shadow_run_id": risk_rejected.run.run_id }),
            ))
            .await
            .expect("risk rejected preview response");
        assert_eq!(risk_rejected_preview.status(), StatusCode::CONFLICT);
        let risk_rejected_error = response_json::<Value>(risk_rejected_preview).await;
        assert_eq!(
            risk_rejected_error["error"],
            "shadow_decision_not_would_submit"
        );

        assert_eq!(count_testnet_shadow_promotions(&test_db.pool).await, 0);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_preview_rejects_kill_switch_and_stale_price_without_persisting() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;

        let kill_switch_shadow =
            create_previewable_shadow_run(&app, &operator_login.access_token).await;
        set_kill_switch_state(
            &test_db.pool,
            &StateActor::system("test"),
            Uuid::new_v4(),
            "shadow promotion preview blocked",
            true,
            Some("manual block".to_string()),
        )
        .await
        .expect("kill switch update");
        let kill_switch_preview = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/promotions/preview",
                &operator_login.access_token,
                json!({ "shadow_run_id": kill_switch_shadow.run.run_id }),
            ))
            .await
            .expect("kill switch preview response");
        assert_eq!(kill_switch_preview.status(), StatusCode::CONFLICT);
        let kill_switch_error = response_json::<Value>(kill_switch_preview).await;
        assert_eq!(kill_switch_error["error"], "kill_switch_active");
        set_kill_switch_state(
            &test_db.pool,
            &StateActor::system("test"),
            Uuid::new_v4(),
            "shadow promotion preview reset",
            false,
            Some("manual reset".to_string()),
        )
        .await
        .expect("kill switch reset");

        let stale_shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        age_shadow_reference_price(&test_db.pool, "BTCUSDT", 120).await;
        let stale_preview = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/shadow/promotions/preview",
                &operator_login.access_token,
                json!({ "shadow_run_id": stale_shadow.run.run_id }),
            ))
            .await
            .expect("stale preview response");
        assert_eq!(stale_preview.status(), StatusCode::CONFLICT);
        let stale_error = response_json::<Value>(stale_preview).await;
        assert_eq!(stale_error["error"], "stale_price");

        assert_eq!(count_testnet_shadow_promotions(&test_db.pool).await, 0);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_creates_isolated_testnet_order_and_lifecycle() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_ack(FakeSubmitAck::default());
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange.clone()),
            FakeExchangeAdapter::status(),
        );
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let submit_response = app
            .oneshot(bearer_request(
                "POST",
                &format!(
                    "/exchange/testnet/shadow/promotions/{}/submit",
                    preview.promotion.promotion_id
                ),
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(submit_response.status(), StatusCode::CREATED);
        let submit = response_json::<TestnetShadowPromotionSubmitResponse>(submit_response).await;
        assert_eq!(count_testnet_shadow_promotions(&test_db.pool).await, 1);
        let orders = list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders");
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].id, submit.result.testnet_order_id);
        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Submitted.as_str()
        );
        assert_eq!(
            persisted.testnet_order_id,
            Some(submit.result.testnet_order_id)
        );
        assert_eq!(
            persisted.client_order_id.as_deref(),
            Some(submit.result.client_order_id.as_str())
        );
        let lifecycle = list_exchange_testnet_order_lifecycle_events(
            &test_db.pool,
            &submit.result.client_order_id,
        )
        .await
        .expect("list lifecycle");
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(lifecycle[0].next_state, "ORDER_SUBMIT_REQUESTED");
        assert_eq!(lifecycle[1].next_state, "EXCHANGE_ACKED");
        assert_eq!(fake_exchange.calls().submitted_orders.len(), 1);
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_rejects_duplicate_submit_without_duplicate_order() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_ack(FakeSubmitAck::default());
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange.clone()),
            FakeExchangeAdapter::status(),
        );
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let submit_uri = format!(
            "/exchange/testnet/shadow/promotions/{}/submit",
            preview.promotion.promotion_id
        );
        let first_submit = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &submit_uri,
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("first submit response");
        assert_eq!(first_submit.status(), StatusCode::CREATED);

        let second_submit = app
            .oneshot(bearer_request(
                "POST",
                &submit_uri,
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("second submit response");
        assert_eq!(second_submit.status(), StatusCode::CONFLICT);
        let error = response_json::<Value>(second_submit).await;
        assert_eq!(error["error"], "duplicate_submit");

        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Submitted.as_str()
        );
        let orders = list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders");
        assert_eq!(orders.len(), 1);
        let lifecycle = list_exchange_testnet_order_lifecycle_events(
            &test_db.pool,
            orders[0].client_order_id.as_str(),
        )
        .await
        .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            system_events_for_order(
                &test_db.pool,
                orders[0].client_order_id.as_str(),
                "exchange.testnet.order.acked"
            )
            .await
            .len(),
            1
        );
        assert_eq!(fake_exchange.calls().submitted_orders.len(), 1);
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_rejects_wrong_confirmation_without_mutation() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let submit_response = app
            .oneshot(bearer_request(
                "POST",
                &format!(
                    "/exchange/testnet/shadow/promotions/{}/submit",
                    preview.promotion.promotion_id
                ),
                &owner_login.access_token,
                json!({ "confirmation_text": "PROMOTE TESTNET ETHUSDT" }),
            ))
            .await
            .expect("submit response");
        assert_eq!(submit_response.status(), StatusCode::BAD_REQUEST);

        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Previewed.as_str()
        );
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_requires_owner_without_mutation() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let submit_response = app
            .oneshot(bearer_request(
                "POST",
                &format!(
                    "/exchange/testnet/shadow/promotions/{}/submit",
                    preview.promotion.promotion_id
                ),
                &operator_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(submit_response.status(), StatusCode::FORBIDDEN);

        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Previewed.as_str()
        );
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_rejects_expired_preview_without_mutation() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        sqlx::query("UPDATE testnet_shadow_promotions SET expires_at = $2 WHERE id = $1")
            .bind(preview.promotion.promotion_id)
            .bind(Utc::now() - chrono::Duration::seconds(1))
            .execute(&test_db.pool)
            .await
            .expect("promotion expiry update");

        let submit_response = app
            .oneshot(bearer_request(
                "POST",
                &format!(
                    "/exchange/testnet/shadow/promotions/{}/submit",
                    preview.promotion.promotion_id
                ),
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(submit_response.status(), StatusCode::CONFLICT);
        let error = response_json::<Value>(submit_response).await;
        assert_eq!(error["error"], "promotion_expired");

        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Expired.as_str()
        );
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_uses_persisted_promotion_payload() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_ack(FakeSubmitAck::default());
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange.clone()),
            FakeExchangeAdapter::status(),
        );
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let expected_side = preview.promotion.would_submit_payload.side;
        let mut mutated_payload = serde_json::to_value(&preview.promotion.would_submit_payload)
            .expect("payload should serialize");
        mutated_payload["side"] = Value::String("SELL".to_string());
        mutated_payload["quote_notional"] = Value::String("123456".to_string());
        sqlx::query("UPDATE testnet_shadow_runs SET would_submit_payload = $2 WHERE id = $1")
            .bind(shadow.run.run_id)
            .bind(mutated_payload)
            .execute(&test_db.pool)
            .await
            .expect("shadow run payload update");

        let submit_response = app
            .oneshot(bearer_request(
                "POST",
                &format!(
                    "/exchange/testnet/shadow/promotions/{}/submit",
                    preview.promotion.promotion_id
                ),
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(submit_response.status(), StatusCode::CREATED);

        let submitted_orders = fake_exchange.calls().submitted_orders;
        assert_eq!(submitted_orders.len(), 1);
        assert_eq!(submitted_orders[0].side, expected_side);
        assert_ne!(
            submitted_orders[0].quote_notional,
            Some(Decimal::new(123_456, 0))
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn shadow_promotion_submit_adapter_failure_marks_rejected_without_ack() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_submit_error(aegis_core::ExchangeError::Transport(
            "deterministic timeout".to_string(),
        ));
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange.clone()),
            FakeExchangeAdapter::status(),
        );
        let app = shadow_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        setup_shadow_promotion_fixture(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let shadow = create_previewable_shadow_run(&app, &operator_login.access_token).await;
        let preview =
            create_shadow_promotion_preview(&app, &operator_login.access_token, shadow.run.run_id)
                .await;

        let submit_uri = format!(
            "/exchange/testnet/shadow/promotions/{}/submit",
            preview.promotion.promotion_id
        );
        let failed_submit = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &submit_uri,
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("submit response");
        assert_eq!(failed_submit.status(), StatusCode::CONFLICT);

        let persisted =
            shadow_promotion_record(&test_db.pool, preview.promotion.promotion_id).await;
        assert_eq!(
            persisted.status,
            TestnetShadowPromotionStatus::Rejected.as_str()
        );
        let order = list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .into_iter()
            .next()
            .expect("testnet order should exist");
        let lifecycle = list_exchange_testnet_order_lifecycle_events(
            &test_db.pool,
            order.client_order_id.as_str(),
        )
        .await
        .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].next_state, "ORDER_SUBMIT_REQUESTED");
        assert!(system_events_for_order(
            &test_db.pool,
            order.client_order_id.as_str(),
            "exchange.testnet.order.acked"
        )
        .await
        .is_empty());

        let repeat_submit = app
            .oneshot(bearer_request(
                "POST",
                &submit_uri,
                &owner_login.access_token,
                json!({
                    "confirmation_text": expected_testnet_shadow_promotion_confirmation("BTCUSDT")
                }),
            ))
            .await
            .expect("repeat submit response");
        assert_eq!(repeat_submit.status(), StatusCode::CONFLICT);
        let repeat_error = response_json::<Value>(repeat_submit).await;
        assert_eq!(repeat_error["error"], "duplicate_submit");

        assert_eq!(fake_exchange.calls().submitted_orders.len(), 1);
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_cancel_happy_path_with_fake_adapter_updates_lifecycle() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        fake_exchange.push_cancel_ack(exchange::testing::FakeCancelAck::default());
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange),
            FakeExchangeAdapter::status(),
        );
        let app = repair_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let client_order_id = "cancel-happy-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "ACKED",
                TestnetExecutionState::ExchangeAcked,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/cancel"),
                &owner_login.access_token,
                json!({
                    "confirmation_text": "TESTNET ORDER"
                }),
            ))
            .await
            .expect("cancel response");
        assert_eq!(response.status(), StatusCode::OK);

        let order = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query")
            .expect("order should exist");
        assert_eq!(order.execution_state, "CANCELLED");
        let lifecycle =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
                .await
                .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(lifecycle[0].next_state, "CANCEL_REQUESTED");
        assert_eq!(lifecycle[1].next_state, "CANCELLED");
        assert_eq!(
            system_events_for_order(
                &test_db.pool,
                client_order_id,
                "exchange.testnet.order.cancel_requested",
            )
            .await
            .len(),
            1
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn reconciliation_happy_path_with_fake_adapter_updates_order_and_run() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        let client_order_id = "reconcile-happy-client-1";
        fake_exchange.set_order_status(
            client_order_id,
            FakeOrderStatus::new(ExchangeOrderState::Filled),
        );
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "NEW",
                TestnetExecutionState::New,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let request = aegis_core::ExchangeReconciliationRequest {
            exchange: aegis_core::ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            limit: 20,
            status_filter: vec!["NEW".to_string()],
            correlation_id: Some(Uuid::new_v4()),
        };
        let details = reconcile_testnet_orders(
            &test_db.pool,
            &fake_exchange,
            "aegis-test-api",
            &StateActor::system("test"),
            &request,
        )
        .await
        .expect("reconciliation should succeed");

        assert_eq!(details.run.status.as_str(), "COMPLETED");
        let order = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query")
            .expect("order should exist");
        assert_eq!(order.execution_state, "FILLED");
        let lifecycle =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
                .await
                .expect("lifecycle events");
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].transition_source, "REST_RECONCILIATION");
        let runs = list_exchange_reconciliation_runs(
            &test_db.pool,
            ExchangeEnvironment::Testnet.as_str(),
            10,
        )
        .await
        .expect("reconciliation runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "COMPLETED");
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn reconciliation_mismatch_path_with_fake_adapter_marks_reconciliation_required() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        let client_order_id = "reconcile-mismatch-client-1";
        fake_exchange.set_order_status(
            client_order_id,
            FakeOrderStatus::new(ExchangeOrderState::New),
        );
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "FILLED",
                TestnetExecutionState::Filled,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let request = aegis_core::ExchangeReconciliationRequest {
            exchange: aegis_core::ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            limit: 20,
            status_filter: vec!["FILLED".to_string()],
            correlation_id: Some(Uuid::new_v4()),
        };
        let _ = reconcile_testnet_orders(
            &test_db.pool,
            &fake_exchange,
            "aegis-test-api",
            &StateActor::system("test"),
            &request,
        )
        .await
        .expect("reconciliation should succeed");

        let order = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query")
            .expect("order should exist");
        assert_eq!(order.execution_state, "RECONCILIATION_REQUIRED");
        let mismatches = list_exchange_reconciliation_mismatches(
            &test_db.pool,
            list_exchange_reconciliation_runs(
                &test_db.pool,
                ExchangeEnvironment::Testnet.as_str(),
                10,
            )
            .await
            .expect("reconciliation runs")[0]
                .id,
        )
        .await
        .expect("reconciliation mismatches");
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].mismatch_kind, "STATUS_MISMATCH");
        let lifecycle =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
                .await
                .expect("lifecycle events");
        assert_eq!(lifecycle[0].next_state, "RECONCILIATION_REQUIRED");
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn manual_recheck_repair_with_fake_adapter_applies_transition() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake_exchange = FakeExchangeAdapter::new();
        let client_order_id = "manual-recheck-client-1";
        fake_exchange.set_order_status(
            client_order_id,
            FakeOrderStatus::new(ExchangeOrderState::Canceled),
        );
        let state = auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake_exchange),
            FakeExchangeAdapter::status(),
        );
        let app = repair_test_router(state);
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "RECONCILIATION_REQUIRED",
                TestnetExecutionState::ReconciliationRequired,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");
        let (login_payload, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "MANUAL_RECHECK",
                    "confirmation_text": format!("REPAIR TESTNET {client_order_id}"),
                    "reason": "deterministic_manual_recheck"
                }),
            ))
            .await
            .expect("manual recheck response");
        assert_eq!(response.status(), StatusCode::OK);

        let order = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query")
            .expect("order should exist");
        assert_eq!(order.execution_state, "CANCELLED");
        let repairs = list_exchange_testnet_repair_actions(&test_db.pool, client_order_id)
            .await
            .expect("repair rows");
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].action, "MANUAL_RECHECK");
        assert_eq!(repairs[0].status, "APPLIED");
        assert_eq!(repairs[0].next_state.as_deref(), Some("CANCELLED"));
        assert_eq!(
            system_events_for_order(
                &test_db.pool,
                client_order_id,
                "exchange.testnet.repair.applied"
            )
            .await
            .len(),
            1
        );
        assert_no_paper_or_backtest_mutation(&test_db.pool).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_preview_rejects_stale_price_without_persistence() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        let stale_received_at = Utc::now() - chrono::Duration::seconds(30);
        let stale_tick = MarketTick {
            received_at: stale_received_at,
            trade_time: stale_received_at,
            ..sample_market_tick("BTCUSDT", Decimal::new(100_000, 0))
        };
        insert_market_tick(&test_db.pool, &stale_tick)
            .await
            .expect("stale market tick");

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/preview",
                &owner_login.access_token,
                json!({ "risk_decision_id": risk_decision_id }),
            ))
            .await
            .expect("preview response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());
        assert_eq!(
            count_exchange_testnet_lifecycle_events(&test_db.pool).await,
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn testnet_pipeline_preview_uses_recent_closed_candle_when_tick_missing() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(test_db.pool.clone(), None, None);
        let app = pipeline_test_router(state);
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let risk_decision_id = insert_test_risk_decision(
            &test_db.pool,
            "APPROVED",
            "BTCUSDT",
            "buy",
            Decimal::new(100_000, 0),
        )
        .await;
        insert_recent_closed_candle(&test_db.pool, "BTCUSDT", Decimal::new(99_500, 0)).await;

        let response = app
            .oneshot(bearer_request(
                "POST",
                "/exchange/testnet/pipeline/preview",
                &owner_login.access_token,
                json!({ "risk_decision_id": risk_decision_id }),
            ))
            .await
            .expect("preview response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<ExchangeTestnetPipelinePreviewResponse>(response).await;
        assert_eq!(payload.preview.reference_price, Decimal::new(99_500, 0));
        assert!(list_exchange_testnet_orders(&test_db.pool, 20)
            .await
            .expect("list testnet orders")
            .is_empty());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn bootstrap_owner_persists_user_and_audit() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state);

        let response = app
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("bootstrap response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let payload = response_json::<AuthUserResponse>(response).await;

        let user = get_user_by_email(&test_db.pool, "owner@example.com")
            .await
            .expect("user query")
            .expect("owner should exist");
        assert_eq!(payload.user.id, user.id);
        assert_eq!(user.role, UserRole::Owner.as_str());
        assert_ne!(user.password_hash, "replace-with-a-12-char-min-password");
        assert_eq!(
            count_system_events(&test_db.pool, "auth.owner_bootstrapped").await,
            1
        );
        assert_eq!(
            count_audit_logs(&test_db.pool, "auth.owner_bootstrapped").await,
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn bootstrap_owner_only_allowed_once() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state);

        let first = app
            .clone()
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("first bootstrap");
        assert_eq!(first.status(), StatusCode::CREATED);

        let second = app
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("second bootstrap");
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(count_users(&test_db.pool).await.expect("user count"), 1);
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn login_creates_session_and_updates_last_login() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state.clone());
        let _ = app
            .clone()
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("bootstrap response");

        let (payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let claims = decode_access_token(&state.auth_config, &payload.access_token)
            .expect("access token claims");
        let session_id = Uuid::parse_str(&claims.session_id).expect("session id");
        let session = get_session_by_id(&test_db.pool, session_id)
            .await
            .expect("session query")
            .expect("session should exist");
        let user = get_user_by_email(&test_db.pool, "owner@example.com")
            .await
            .expect("user query")
            .expect("owner should exist");

        assert!(payload.refresh_token.is_some());
        assert_eq!(session.user_id, user.id);
        assert_ne!(
            session.refresh_token_hash,
            payload.refresh_token.expect("refresh token")
        );
        assert!(user.last_login_at.is_some());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn login_rejects_wrong_password_without_session() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state);
        let _ = app
            .clone()
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("bootstrap response");

        let response = app
            .oneshot(cli_request(
                "POST",
                "/auth/login",
                json!({ "email": "owner@example.com", "password": "wrong-password-value" }),
            ))
            .await
            .expect("login response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            count_system_events(&test_db.pool, "auth.login.failed").await,
            1
        );
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS count FROM sessions")
                .fetch_one(&test_db.pool)
                .await
                .expect("session count")
                .get::<i64, _>("count"),
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn refresh_rotates_session_and_issues_new_access_token() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state.clone());
        let _ = app
            .clone()
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("bootstrap response");

        let (login_payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let original_claims = decode_access_token(&state.auth_config, &login_payload.access_token)
            .expect("original claims");
        let original_session = get_session_by_id(
            &test_db.pool,
            Uuid::parse_str(&original_claims.session_id).expect("session id"),
        )
        .await
        .expect("session query")
        .expect("session exists");

        let refresh_response = app
            .clone()
            .oneshot(cli_request(
                "POST",
                "/auth/refresh",
                json!({ "refresh_token": login_payload.refresh_token.clone().expect("refresh token") }),
            ))
            .await
            .expect("refresh response");
        assert_eq!(refresh_response.status(), StatusCode::OK);
        let refreshed = response_json::<AuthRefreshResponse>(refresh_response).await;
        let refreshed_claims = decode_access_token(&state.auth_config, &refreshed.access_token)
            .expect("refreshed claims");
        let rotated_session = get_session_by_id(
            &test_db.pool,
            Uuid::parse_str(&refreshed_claims.session_id).expect("session id"),
        )
        .await
        .expect("session query")
        .expect("rotated session exists");

        assert_eq!(original_session.id, rotated_session.id);
        assert_ne!(
            original_session.refresh_token_hash,
            rotated_session.refresh_token_hash
        );
        assert_ne!(
            login_payload.refresh_token.expect("original refresh"),
            refreshed.refresh_token.expect("new refresh")
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn logout_revokes_session_and_blocks_refresh() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let state = auth_test_state(
            test_db.pool.clone(),
            Some("owner@example.com"),
            Some("replace-with-a-12-char-min-password"),
        );
        let app = auth_test_router(state.clone());
        let _ = app
            .clone()
            .oneshot(cli_request("POST", "/auth/bootstrap-owner", json!({})))
            .await
            .expect("bootstrap response");

        let (login_payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let claims =
            decode_access_token(&state.auth_config, &login_payload.access_token).expect("claims");
        let session_id = Uuid::parse_str(&claims.session_id).expect("session id");

        let logout_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/auth/logout",
                &login_payload.access_token,
                json!({}),
            ))
            .await
            .expect("logout response");
        assert_eq!(logout_response.status(), StatusCode::OK);
        let _ = response_json::<AuthLogoutResponse>(logout_response).await;

        let session = get_session_by_id(&test_db.pool, session_id)
            .await
            .expect("session query")
            .expect("session exists");
        assert!(session.revoked_at.is_some());

        let refresh_response = app
            .oneshot(cli_request(
                "POST",
                "/auth/refresh",
                json!({ "refresh_token": login_payload.refresh_token.expect("refresh token") }),
            ))
            .await
            .expect("refresh response");
        assert_eq!(refresh_response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn protected_mutating_endpoint_rejects_unauthenticated() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = auth_test_router(auth_test_state(test_db.pool.clone(), None, None));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/risk/resume")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn role_authorization_enforces_viewer_operator_and_owner() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = auth_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Viewer,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;

        let (viewer_login, _) = login_cli(
            &app,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let viewer_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/strategy/momentum_v1/enable",
                &viewer_login.access_token,
                json!({}),
            ))
            .await
            .expect("viewer response");
        assert_eq!(viewer_response.status(), StatusCode::FORBIDDEN);

        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let operator_response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                "/risk/resume",
                &operator_login.access_token,
                json!({}),
            ))
            .await
            .expect("operator response");
        assert_eq!(operator_response.status(), StatusCode::FORBIDDEN);

        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let owner_response = app
            .oneshot(bearer_request(
                "POST",
                "/risk/resume",
                &owner_login.access_token,
                json!({}),
            ))
            .await
            .expect("owner response");
        assert_eq!(owner_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn repair_action_persists_to_database() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        let user = get_user_by_email(&test_db.pool, "owner@example.com")
            .await
            .expect("user query")
            .expect("user should exist");

        let client_order_id = "repair-persist-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "UNKNOWN_EXCHANGE_STATE",
                TestnetExecutionState::UnknownExchangeState,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let (login_payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "MARK_FAILED",
                    "confirmation_text": format!("REPAIR TESTNET {client_order_id}"),
                    "reason": "operator_marked_failed_for_test"
                }),
            ))
            .await
            .expect("repair response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<Value>(response).await;

        assert_eq!(
            payload.get("action").and_then(Value::as_str),
            Some("MARK_FAILED")
        );
        assert_eq!(
            payload.get("status").and_then(Value::as_str),
            Some("APPLIED")
        );

        let repairs = list_exchange_testnet_repair_actions(&test_db.pool, client_order_id)
            .await
            .expect("repair actions should list");
        assert_eq!(repairs.len(), 1);
        let repair = &repairs[0];
        assert_eq!(repair.action, "MARK_FAILED");
        assert_eq!(repair.status, "APPLIED");
        assert_eq!(
            repair.previous_state.as_deref(),
            Some("UNKNOWN_EXCHANGE_STATE")
        );
        assert_eq!(repair.next_state.as_deref(), Some("FAILED"));
        assert_eq!(
            repair.reason.as_deref(),
            Some("operator_marked_failed_for_test")
        );
        assert_eq!(repair.actor_id, Some(user.id));
        let correlation_id = payload
            .get("correlation_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok());
        assert_eq!(repair.correlation_id, correlation_id);
        assert!(repair.created_at <= Utc::now());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn repair_appends_lifecycle_event_and_moves_to_reconciliation_required() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;

        let client_order_id = "repair-lifecycle-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "RECONCILIATION_REQUIRED",
                TestnetExecutionState::UnknownExchangeState,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let (login_payload, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "MARK_RECONCILIATION_REQUIRED",
                    "confirmation_text": format!("REPAIR TESTNET {client_order_id}"),
                    "reason": "needs_manual_reconciliation"
                }),
            ))
            .await
            .expect("repair response");
        assert_eq!(response.status(), StatusCode::OK);

        let events = list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
            .await
            .expect("lifecycle events should list");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].transition_source,
            "OPERATOR_MARK_RECONCILIATION_REQUIRED"
        );
        assert_eq!(
            events[0].previous_state.as_deref(),
            Some("UNKNOWN_EXCHANGE_STATE")
        );
        assert_eq!(events[0].next_state, "RECONCILIATION_REQUIRED");

        let updated = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query should succeed")
            .expect("order should exist");
        assert_eq!(updated.execution_state, "RECONCILIATION_REQUIRED");
        assert_eq!(updated.status, "RECONCILIATION_REQUIRED");
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn repair_writes_audit_and_system_events() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;

        let client_order_id = "repair-audit-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "RECONCILIATION_REQUIRED",
                TestnetExecutionState::Failed,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let (login_payload, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "MARK_RECONCILIATION_REQUIRED",
                    "confirmation_text": format!("REPAIR TESTNET {client_order_id}"),
                    "reason": "audit_and_system_event_test"
                }),
            ))
            .await
            .expect("repair response");
        assert_eq!(response.status(), StatusCode::OK);

        let user = get_user_by_email(&test_db.pool, "operator@example.com")
            .await
            .expect("user query")
            .expect("user should exist");
        let (actor, metadata) = latest_audit_log_for_target(
            &test_db.pool,
            "exchange.testnet.repair.requested",
            client_order_id,
        )
        .await;
        assert_eq!(actor, "user:operator@example.com");
        assert_eq!(
            metadata.get("action").and_then(Value::as_str),
            Some("MARK_RECONCILIATION_REQUIRED")
        );
        assert_eq!(metadata.get("force").and_then(Value::as_bool), Some(false));

        let repair_rows = list_exchange_testnet_repair_actions(&test_db.pool, client_order_id)
            .await
            .expect("repair rows should list");
        assert_eq!(repair_rows[0].actor_id, Some(user.id));

        let requested = system_events_for_order(
            &test_db.pool,
            client_order_id,
            "exchange.testnet.repair.requested",
        )
        .await;
        let applied = system_events_for_order(
            &test_db.pool,
            client_order_id,
            "exchange.testnet.repair.applied",
        )
        .await;
        assert_eq!(requested.len(), 1);
        assert_eq!(applied.len(), 1);
        assert_eq!(
            requested[0].get("action").and_then(Value::as_str),
            Some("MARK_RECONCILIATION_REQUIRED")
        );
        assert_eq!(
            applied[0].get("next_state").and_then(Value::as_str),
            Some("RECONCILIATION_REQUIRED")
        );

        let requested_payload_text = requested[0].to_string();
        let applied_payload_text = applied[0].to_string();
        assert!(!requested_payload_text.contains("api_secret"));
        assert!(!requested_payload_text.contains("api_key"));
        assert!(!applied_payload_text.contains("api_secret"));
        assert!(!applied_payload_text.contains("api_key"));
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn rejected_repair_persists_rejection_without_lifecycle_transition() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;

        let client_order_id = "repair-rejected-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "FILLED",
                TestnetExecutionState::Filled,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let (login_payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "MARK_ACKED",
                    "confirmation_text": format!("REPAIR TESTNET {client_order_id}"),
                    "reason": "invalid_terminal_transition"
                }),
            ))
            .await
            .expect("repair response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json::<Value>(response).await;
        assert_eq!(
            payload.get("status").and_then(Value::as_str),
            Some("REJECTED")
        );

        let repairs = list_exchange_testnet_repair_actions(&test_db.pool, client_order_id)
            .await
            .expect("repair rows should list");
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].status, "REJECTED");
        assert_eq!(repairs[0].action, "MARK_ACKED");
        assert_eq!(repairs[0].next_state.as_deref(), Some("EXCHANGE_ACKED"));

        let rejected = system_events_for_order(
            &test_db.pool,
            client_order_id,
            "exchange.testnet.repair.rejected",
        )
        .await;
        assert_eq!(rejected.len(), 1);
        assert_eq!(
            rejected[0].get("reason").and_then(Value::as_str),
            Some("invalid_repair_transition")
        );

        let lifecycle_events =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
                .await
                .expect("lifecycle events should list");
        assert!(lifecycle_events.is_empty());

        let order = get_exchange_testnet_order_by_client_order_id(&test_db.pool, client_order_id)
            .await
            .expect("order query should succeed")
            .expect("order should exist");
        assert_eq!(order.execution_state, "FILLED");
        assert_eq!(order.status, "FILLED");
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn repair_history_listing_is_isolated_per_client_order_id() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        let user = get_user_by_email(&test_db.pool, "operator@example.com")
            .await
            .expect("user query")
            .expect("user should exist");

        let target_client_order_id = "repair-history-target";
        let other_client_order_id = "repair-history-other";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                target_client_order_id,
                "FAILED",
                TestnetExecutionState::Failed,
                "BTCUSDT",
            ),
        )
        .await
        .expect("target order insert");
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                other_client_order_id,
                "FAILED",
                TestnetExecutionState::Failed,
                "BTCUSDT",
            ),
        )
        .await
        .expect("other order insert");

        let older_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap();
        let newer_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 2).unwrap();
        insert_exchange_testnet_repair_action(
            &test_db.pool,
            &db::ExchangeTestnetRepairActionRecord {
                id: Uuid::new_v4(),
                client_order_id: target_client_order_id.to_string(),
                action: "MARK_FAILED".to_string(),
                status: "APPLIED".to_string(),
                previous_state: Some("UNKNOWN_EXCHANGE_STATE".to_string()),
                next_state: Some("FAILED".to_string()),
                reason: Some("older".to_string()),
                payload: Some(json!({ "force": false })),
                actor_id: Some(user.id),
                created_at: older_time,
                correlation_id: Some(Uuid::new_v4()),
            },
        )
        .await
        .expect("older repair insert");
        insert_exchange_testnet_repair_action(
            &test_db.pool,
            &db::ExchangeTestnetRepairActionRecord {
                id: Uuid::new_v4(),
                client_order_id: target_client_order_id.to_string(),
                action: "MARK_RECONCILIATION_REQUIRED".to_string(),
                status: "APPLIED".to_string(),
                previous_state: Some("FAILED".to_string()),
                next_state: Some("RECONCILIATION_REQUIRED".to_string()),
                reason: Some("newer".to_string()),
                payload: Some(json!({ "force": false })),
                actor_id: Some(user.id),
                created_at: newer_time,
                correlation_id: Some(Uuid::new_v4()),
            },
        )
        .await
        .expect("newer repair insert");
        insert_exchange_testnet_repair_action(
            &test_db.pool,
            &db::ExchangeTestnetRepairActionRecord {
                id: Uuid::new_v4(),
                client_order_id: other_client_order_id.to_string(),
                action: "MARK_FAILED".to_string(),
                status: "REJECTED".to_string(),
                previous_state: Some("FILLED".to_string()),
                next_state: Some("FAILED".to_string()),
                reason: Some("other-order".to_string()),
                payload: Some(json!({ "force": false })),
                actor_id: Some(user.id),
                created_at: newer_time,
                correlation_id: Some(Uuid::new_v4()),
            },
        )
        .await
        .expect("other repair insert");

        let (login_payload, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .oneshot(bearer_request(
                "GET",
                &format!("/exchange/testnet/orders/{target_client_order_id}/repairs"),
                &login_payload.access_token,
                json!({}),
            ))
            .await
            .expect("repairs response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json::<Value>(response).await;
        let repairs = payload
            .get("repairs")
            .and_then(Value::as_array)
            .expect("repairs array should exist");

        assert_eq!(
            payload.get("client_order_id").and_then(Value::as_str),
            Some(target_client_order_id)
        );
        assert_eq!(repairs.len(), 2);
        assert!(repairs.iter().all(|repair| {
            repair.get("client_order_id").and_then(Value::as_str) == Some(target_client_order_id)
        }));
        assert_eq!(
            repairs[0].get("reason").and_then(Value::as_str),
            Some("newer")
        );
        assert_eq!(
            repairs[1].get("reason").and_then(Value::as_str),
            Some("older")
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn safe_cancel_validation_persists_without_network_dependency() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = repair_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;

        let client_order_id = "repair-safe-cancel-client-1";
        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                client_order_id,
                "FILLED",
                TestnetExecutionState::Filled,
                "BTCUSDT",
            ),
        )
        .await
        .expect("order insert");

        let (login_payload, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let response = app
            .clone()
            .oneshot(bearer_request(
                "POST",
                &format!("/exchange/testnet/orders/{client_order_id}/repair"),
                &login_payload.access_token,
                json!({
                    "action": "SAFE_CANCEL_REQUEST",
                    "confirmation_text": format!("CANCEL TESTNET {client_order_id}"),
                    "reason": "validation_only"
                }),
            ))
            .await
            .expect("safe cancel response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json::<Value>(response).await;
        assert_eq!(
            payload.get("status").and_then(Value::as_str),
            Some("REJECTED")
        );

        let repairs = list_exchange_testnet_repair_actions(&test_db.pool, client_order_id)
            .await
            .expect("repair rows should list");
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].action, "SAFE_CANCEL_REQUEST");
        assert_eq!(repairs[0].status, "REJECTED");
        assert_eq!(repairs[0].next_state.as_deref(), Some("CANCEL_REQUESTED"));

        let cancel_requested = system_events_for_order(
            &test_db.pool,
            client_order_id,
            "exchange.testnet.repair.cancel_requested",
        )
        .await;
        let rejected = system_events_for_order(
            &test_db.pool,
            client_order_id,
            "exchange.testnet.repair.rejected",
        )
        .await;
        assert_eq!(cancel_requested.len(), 1);
        assert_eq!(rejected.len(), 1);

        let lifecycle_events =
            list_exchange_testnet_order_lifecycle_events(&test_db.pool, client_order_id)
                .await
                .expect("lifecycle events should list");
        assert!(lifecycle_events.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn readiness_snapshot_persistence_list_and_get_work() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = readiness_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Viewer,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        upsert_risk_config(&test_db.pool, &readiness_risk_config())
            .await
            .expect("risk config");
        seed_readiness_market(&test_db.pool, "BTCUSDT").await;
        insert_active_paper_account(&test_db.pool).await;
        let before = readiness_mutation_counts(&test_db.pool).await;

        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (viewer_login, _) = login_cli(
            &app,
            "viewer@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let first = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &operator_login.access_token,
                    json!({
                        "target": "PAPER_PIPELINE",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "persist": true
                    }),
                ))
                .await
                .expect("first readiness response"),
        )
        .await;
        let second = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &operator_login.access_token,
                    json!({
                        "target": "PAPER_PIPELINE",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "persist": true
                    }),
                ))
                .await
                .expect("second readiness response"),
        )
        .await;

        assert_eq!(count_execution_readiness_snapshots(&test_db.pool).await, 2);
        assert_readiness_read_only(&test_db.pool, 2, before).await;

        let listed = response_json::<ExecutionReadinessSnapshotsResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "GET",
                    "/readiness/snapshots?limit=1",
                    &viewer_login.access_token,
                    json!({}),
                ))
                .await
                .expect("snapshot list response"),
        )
        .await;
        assert_eq!(listed.snapshots.len(), 1);
        assert_eq!(listed.snapshots[0].id, second.readiness.readiness_id);

        let fetched = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "GET",
                    &format!("/readiness/snapshots/{}", first.readiness.readiness_id),
                    &viewer_login.access_token,
                    json!({}),
                ))
                .await
                .expect("snapshot get response"),
        )
        .await;
        assert_eq!(fetched.readiness.readiness_id, first.readiness.readiness_id);
        assert_eq!(fetched.readiness.status, first.readiness.status);
        assert_eq!(fetched.readiness.score, first.readiness.score);
        assert_eq!(
            fetched.readiness.blocking_reasons,
            first.readiness.blocking_reasons
        );
        assert_eq!(fetched.readiness.checks, first.readiness.checks);
        assert_eq!(
            fetched.readiness.recommendations,
            first.readiness.recommendations
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn readiness_paper_pipeline_status_matrix_is_read_only() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = readiness_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        upsert_risk_config(&test_db.pool, &readiness_risk_config())
            .await
            .expect("risk config");
        seed_readiness_market(&test_db.pool, "BTCUSDT").await;
        insert_active_paper_account(&test_db.pool).await;
        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(false))
            .await
            .expect("disabled strategy");
        let before_disabled = readiness_mutation_counts(&test_db.pool).await;
        let disabled = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &operator_login.access_token,
                    json!({
                        "target": "PAPER_PIPELINE",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m"
                    }),
                ))
                .await
                .expect("disabled readiness response"),
        )
        .await;
        assert_eq!(
            disabled.readiness.status,
            ExecutionReadinessStatus::NotReady
        );
        assert!(disabled
            .readiness
            .blocking_reasons
            .contains(&aegis_core::ExecutionReadinessBlockingReason::StrategyDisabled));
        assert_readiness_read_only(&test_db.pool, 0, before_disabled).await;

        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("enabled strategy");
        let before_healthy = readiness_mutation_counts(&test_db.pool).await;
        let healthy = response_json::<ExecutionReadinessResponse>(
            app.oneshot(bearer_request(
                "POST",
                "/readiness/check",
                &operator_login.access_token,
                json!({
                    "target": "PAPER_PIPELINE",
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m"
                }),
            ))
            .await
            .expect("healthy readiness response"),
        )
        .await;
        assert!(matches!(
            healthy.readiness.status,
            ExecutionReadinessStatus::Ready | ExecutionReadinessStatus::Degraded
        ));
        for blocker in [
            aegis_core::ExecutionReadinessBlockingReason::StrategyDisabled,
            aegis_core::ExecutionReadinessBlockingReason::StaleMarketFeed,
            aegis_core::ExecutionReadinessBlockingReason::MissingRecentMarketPrice,
            aegis_core::ExecutionReadinessBlockingReason::PaperAccountMissing,
            aegis_core::ExecutionReadinessBlockingReason::PaperAccountUnhealthy,
        ] {
            assert!(!healthy.readiness.blocking_reasons.contains(&blocker));
        }
        assert_readiness_read_only(&test_db.pool, 0, before_healthy).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn readiness_target_specific_blockers_are_classified_without_execution_mutation() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let app = readiness_test_router(auth_test_state(test_db.pool.clone(), None, None));
        insert_test_user(
            &test_db.pool,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Operator,
        )
        .await;
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        upsert_strategy_config(&test_db.pool, &shadow_strategy_config(true))
            .await
            .expect("strategy config");
        upsert_risk_config(&test_db.pool, &readiness_risk_config())
            .await
            .expect("risk config");
        seed_readiness_market(&test_db.pool, "BTCUSDT").await;
        upsert_testnet_shadow_runner_state(
            &test_db.pool,
            &TestnetShadowRunnerState {
                id: db::TESTNET_SHADOW_RUNNER_STATE_ID,
                status: TestnetShadowRunnerStatus::Paused,
                last_tick_at: Some(Utc::now()),
                last_success_at: Some(Utc::now()),
                last_error: None,
                total_ticks: 1,
                total_runs: 1,
                updated_at: Utc::now(),
            },
        )
        .await
        .expect("runner state");
        upsert_exchange_private_stream_state(
            &test_db.pool,
            &ExchangePrivateStreamStateRecord {
                exchange: "binance".to_string(),
                environment: "testnet".to_string(),
                status: "CONNECTED".to_string(),
                listen_key_hash: Some("hash".to_string()),
                connected_at: Some(Utc::now()),
                last_event_at: Some(Utc::now()),
                last_error: None,
                reconnect_count: 0,
                updated_at: Utc::now(),
            },
        )
        .await
        .expect("private stream");
        let risk_decision_id =
            insert_test_risk_decision(&test_db.pool, "approved", "BTCUSDT", "BUY", Decimal::ONE)
                .await;

        let (operator_login, _) = login_cli(
            &app,
            "operator@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let before_shadow = readiness_mutation_counts(&test_db.pool).await;
        let shadow = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &operator_login.access_token,
                    json!({
                        "target": "TESTNET_SHADOW",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m"
                    }),
                ))
                .await
                .expect("shadow readiness response"),
        )
        .await;
        assert!(matches!(
            shadow.readiness.status,
            ExecutionReadinessStatus::Degraded | ExecutionReadinessStatus::NotReady
        ));
        assert!(shadow
            .readiness
            .warnings
            .iter()
            .any(|warning| warning.summary.contains("paused or stopped")));
        assert_readiness_read_only(&test_db.pool, 0, before_shadow).await;

        let before_promotion = readiness_mutation_counts(&test_db.pool).await;
        let promotion = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &operator_login.access_token,
                    json!({
                        "target": "TESTNET_PROMOTION",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m"
                    }),
                ))
                .await
                .expect("promotion readiness response"),
        )
        .await;
        assert_eq!(
            promotion.readiness.status,
            ExecutionReadinessStatus::NotReady
        );
        assert!(promotion
            .readiness
            .blocking_reasons
            .contains(&aegis_core::ExecutionReadinessBlockingReason::ZeroShadowWouldSubmitCount));
        assert!(promotion
            .readiness
            .recommendations
            .contains(&ExecutionReadinessRecommendation::IncreaseShadowCoverage));
        assert_readiness_read_only(&test_db.pool, 0, before_promotion).await;

        insert_exchange_testnet_order(
            &test_db.pool,
            &sample_testnet_order(
                "recon-required",
                "NEW",
                TestnetExecutionState::ReconciliationRequired,
                "BTCUSDT",
            ),
        )
        .await
        .expect("testnet order");
        let before_submit = readiness_mutation_counts(&test_db.pool).await;
        let submit = response_json::<ExecutionReadinessResponse>(
            app.clone()
                .oneshot(bearer_request(
                    "POST",
                    "/readiness/check",
                    &owner_login.access_token,
                    json!({
                        "target": "TESTNET_SUBMIT",
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "risk_decision_id": risk_decision_id
                    }),
                ))
                .await
                .expect("submit readiness response"),
        )
        .await;
        assert_eq!(submit.readiness.status, ExecutionReadinessStatus::NotReady);
        assert!(submit.readiness.blocking_reasons.contains(
            &aegis_core::ExecutionReadinessBlockingReason::ReconciliationRequiredOrdersPresent
        ));
        assert!(submit
            .readiness
            .recommendations
            .contains(&ExecutionReadinessRecommendation::ReconcileTestnetOrders));
        assert_readiness_read_only(&test_db.pool, 0, before_submit).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
    async fn readiness_submit_blocks_stale_private_stream_and_missing_adapter_config() {
        let test_db = TestDatabase::setup().await.expect("test db");
        let fake = FakeExchangeAdapter::new();
        let mut status = FakeExchangeAdapter::status();
        status.configured = false;
        let app = readiness_test_router(auth_test_state_with_adapter(
            test_db.pool.clone(),
            None,
            None,
            None,
            Arc::new(fake),
            status,
        ));
        insert_test_user(
            &test_db.pool,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
            UserRole::Owner,
        )
        .await;
        upsert_risk_config(&test_db.pool, &readiness_risk_config())
            .await
            .expect("risk config");
        seed_readiness_market(&test_db.pool, "BTCUSDT").await;
        upsert_exchange_private_stream_state(
            &test_db.pool,
            &ExchangePrivateStreamStateRecord {
                exchange: "binance".to_string(),
                environment: "testnet".to_string(),
                status: "STALE".to_string(),
                listen_key_hash: Some("hash".to_string()),
                connected_at: Some(Utc::now() - chrono::Duration::minutes(5)),
                last_event_at: Some(Utc::now() - chrono::Duration::minutes(5)),
                last_error: Some("stale".to_string()),
                reconnect_count: 1,
                updated_at: Utc::now(),
            },
        )
        .await
        .expect("private stream");
        let risk_decision_id =
            insert_test_risk_decision(&test_db.pool, "approved", "BTCUSDT", "BUY", Decimal::ONE)
                .await;
        let before = readiness_mutation_counts(&test_db.pool).await;
        let (owner_login, _) = login_cli(
            &app,
            "owner@example.com",
            "replace-with-a-12-char-min-password",
        )
        .await;

        let response = response_json::<ExecutionReadinessResponse>(
            app.oneshot(bearer_request(
                "POST",
                "/readiness/check",
                &owner_login.access_token,
                json!({
                    "target": "TESTNET_SUBMIT",
                    "strategy_id": "momentum_v1",
                    "symbol": "BTCUSDT",
                    "timeframe": "1m",
                    "risk_decision_id": risk_decision_id
                }),
            ))
            .await
            .expect("submit readiness response"),
        )
        .await;

        assert_eq!(
            response.readiness.status,
            ExecutionReadinessStatus::NotReady
        );
        assert!(response
            .readiness
            .blocking_reasons
            .contains(&aegis_core::ExecutionReadinessBlockingReason::PrivateStreamStale));
        assert!(response
            .readiness
            .blocking_reasons
            .contains(&aegis_core::ExecutionReadinessBlockingReason::TestnetAdapterNotConfigured));
        assert!(response
            .readiness
            .recommendations
            .contains(&ExecutionReadinessRecommendation::ReconnectPrivateStream));
        assert_readiness_read_only(&test_db.pool, 0, before).await;
    }

    fn test_app_state() -> AppState {
        let adapter = BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            ws_base_url: "wss://stream.testnet.binance.vision/ws".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        });
        let status = adapter.status();
        AppState {
            config: AppConfig {
                app_name: "aegis-test-api".to_string(),
                environment: "test".to_string(),
                bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
                database_url: "postgres://postgres:postgres@127.0.0.1:5432/aegis".to_string(),
                database_max_connections: 1,
                cors_allowed_origins: parse_cors_allowed_origins(None).expect("origins"),
            },
            auth_config: AuthConfig {
                disabled: true,
                jwt_secret: Some("test-secret".to_string()),
                access_token_ttl: std::time::Duration::from_secs(900),
                refresh_token_ttl: std::time::Duration::from_secs(86_400),
                cookie_secure: false,
                protect_metrics: false,
                bootstrap_owner_email: None,
                bootstrap_owner_password: None,
            },
            db_pool: db::PgPool::connect_lazy("postgres://postgres:postgres@127.0.0.1:5432/aegis")
                .expect("lazy pool"),
            started_at: Utc::now(),
            market_mode: MarketMode::Paper,
            market_config: MarketIngestConfig {
                exchange: MarketDataSource::Binance,
                symbols: vec![Symbol::new("BTCUSDT").expect("symbol")],
                stale_threshold: std::time::Duration::from_secs(10),
                binance_ws_base_url: "wss://example.invalid".to_string(),
                binance_rest_base_url: "https://example.invalid".to_string(),
            },
            strategy_runtime: StrategyRuntimeConfig {
                default_symbols: vec![Symbol::new("BTCUSDT").expect("symbol")],
                default_timeframe: CandleInterval::OneMinute,
                default_notional: Decimal::new(100_000, 0),
                momentum_lookback_candles: 3,
                breakout_lookback_candles: 20,
            },
            exchange_testnet_binance: Some(adapter.clone()),
            exchange_testnet: Arc::new(adapter),
            exchange_testnet_environment: ExchangeEnvironment::Testnet,
            exchange_testnet_status: status,
        }
    }
}
