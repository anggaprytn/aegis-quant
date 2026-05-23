use std::{env, net::SocketAddr, time::Instant};

use aegis_core::MarketMode;
use axum::{
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::Utc;
use db::{check_health, connect_pool, DbConfig, PgPool};
use serde::Serialize;
use tracing::{error, info};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const CORRELATION_ID_HEADER: HeaderName = HeaderName::from_static("x-correlation-id");

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

    let state = AppState {
        config: config.clone(),
        db_pool,
        started_at: Utc::now(),
        market_mode: MarketMode::Paper,
    };

    let app = Router::new()
        .route("/system/health", get(health))
        .route("/system/status", get(status))
        .route("/system/db-health", get(db_health))
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

fn request_context(request: &RequestContext) -> (&str, &str) {
    (&request.request_id, &request.correlation_id)
}

async fn health(
    State(state): State<AppState>,
    request: Option<axum::extract::Extension<RequestContext>>,
) -> Json<HealthResponse> {
    let request = request
        .map(|axum::extract::Extension(value)| value)
        .unwrap_or(RequestContext {
            request_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        });
    let (request_id, correlation_id) = request_context(&request);

    Json(HealthResponse {
        status: "ok",
        service: state.config.app_name,
        environment: state.config.environment,
        request_id: request_id.to_string(),
        correlation_id: correlation_id.to_string(),
        timestamp: Utc::now(),
    })
}

async fn status(
    State(state): State<AppState>,
    request: Option<axum::extract::Extension<RequestContext>>,
) -> Json<StatusResponse> {
    let request = request
        .map(|axum::extract::Extension(value)| value)
        .unwrap_or(RequestContext {
            request_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        });
    let (request_id, correlation_id) = request_context(&request);

    Json(StatusResponse {
        service: state.config.app_name,
        environment: state.config.environment,
        market_mode: state.market_mode,
        started_at: state.started_at,
        request_id: request_id.to_string(),
        correlation_id: correlation_id.to_string(),
        timestamp: Utc::now(),
        dependencies: Dependencies {
            database: DependencyStatus {
                status: "configured",
            },
            event_bus: DependencyStatus {
                status: "not_configured",
            },
            exchange_execution: DependencyStatus { status: "disabled" },
        },
    })
}

async fn db_health(
    State(state): State<AppState>,
    request: Option<axum::extract::Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request
        .map(|axum::extract::Extension(value)| value)
        .unwrap_or(RequestContext {
            request_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        });
    let (request_id, correlation_id) = request_context(&request);

    match check_health(&state.db_pool).await {
        Ok(()) => (
            StatusCode::OK,
            Json(DbHealthResponse {
                status: "ok",
                service: state.config.app_name,
                request_id: request_id.to_string(),
                correlation_id: correlation_id.to_string(),
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request_id,
                correlation_id = %correlation_id,
                error = %err,
                "database health check failed"
            );

            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DbHealthResponse {
                    status: "error",
                    service: state.config.app_name,
                    request_id: request_id.to_string(),
                    correlation_id: correlation_id.to_string(),
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}
