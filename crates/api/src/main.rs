use std::{env, net::SocketAddr, time::Instant};

use aegis_core::{
    MarketMode, OrderIntent, RiskCheckContext, RiskEvaluationDecision, RiskEvaluationResult,
    RiskRejectionReason, Side, Symbol,
};
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
    check_health, connect_pool, create_paper_order, ensure_system_state, get_order_by_id,
    get_system_event, get_system_state, insert_risk_evaluation, list_orders,
    list_recent_system_events, load_risk_state_snapshot, set_kill_switch_state, CreateOrderError,
    DbConfig, OrderRecord, PgPool, StateActor, SystemEventRecord, SystemStateRecord,
};
use events::{EventPublisher, PostgresEventPublisher, SystemEventType};
use risk_engine::RiskEvaluator;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const CORRELATION_ID_HEADER: HeaderName = HeaderName::from_static("x-correlation-id");
const DEFAULT_RECENT_EVENTS_LIMIT: i64 = 50;
const MAX_RECENT_EVENTS_LIMIT: i64 = 200;

#[derive(Clone)]
struct AppState {
    config: AppConfig,
    db_pool: PgPool,
    started_at: chrono::DateTime<Utc>,
    market_mode: MarketMode,
}

#[derive(Clone)]
struct AppConfig {
    app_name: String,
    environment: String,
    bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
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
}

#[derive(Serialize)]
struct RecentEventsResponse {
    events: Vec<SystemEventRecord>,
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
    order: OrderRecord,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct OrdersResponse {
    orders: Vec<OrderRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

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
    };

    let app = Router::new()
        .route("/system/health", get(health))
        .route("/system/status", get(status))
        .route("/system/db-health", get(db_health))
        .route("/events/recent", get(recent_events))
        .route("/events/:id", get(event_by_id))
        .route("/risk/status", get(risk_status))
        .route("/risk/kill-switch", post(enable_kill_switch))
        .route("/risk/resume", post(resume_trading))
        .route("/risk/evaluate", post(evaluate_risk))
        .route("/paper/orders", post(create_order))
        .route("/orders", get(get_orders))
        .route("/orders/:id", get(get_order))
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

fn bounded_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_RECENT_EVENTS_LIMIT),
        _ => DEFAULT_RECENT_EVENTS_LIMIT,
    }
}

fn is_valid_resume_confirmation(value: &str) -> bool {
    value.trim() == "RESUME TRADING"
}

fn default_actor() -> StateActor {
    StateActor::system("anonymous")
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
    let limit = bounded_limit(query.limit);

    match list_recent_system_events(&state.db_pool, limit).await {
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
        Ok(outcome) => (
            StatusCode::CREATED,
            Json(OrderResponse {
                order: outcome.order,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
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

async fn get_orders(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_orders(&state.db_pool).await {
        Ok(orders) => (
            StatusCode::OK,
            Json(OrdersResponse {
                orders,
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
                order,
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

#[cfg(test)]
mod tests {
    use super::{
        bounded_limit, is_valid_resume_confirmation, parse_order_intent, parse_risk_check_context,
        DEFAULT_RECENT_EVENTS_LIMIT, MAX_RECENT_EVENTS_LIMIT,
    };
    use crate::{CreatePaperOrderRequest, RiskEvaluateRequest};
    use aegis_core::Side;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn recent_events_limit_defaults_when_missing_or_invalid() {
        assert_eq!(bounded_limit(None), DEFAULT_RECENT_EVENTS_LIMIT);
        assert_eq!(bounded_limit(Some(0)), DEFAULT_RECENT_EVENTS_LIMIT);
        assert_eq!(bounded_limit(Some(-1)), DEFAULT_RECENT_EVENTS_LIMIT);
    }

    #[test]
    fn recent_events_limit_is_capped() {
        assert_eq!(bounded_limit(Some(25)), 25);
        assert_eq!(bounded_limit(Some(10_000)), MAX_RECENT_EVENTS_LIMIT);
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
}
