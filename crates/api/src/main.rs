use std::{env, net::SocketAddr};

use aegis_core::MarketMode;
use axum::{extract::State, routing::get, Json, Router};
use chrono::Utc;
use serde::Serialize;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Clone)]
struct AppState {
    app_name: String,
    started_at: chrono::DateTime<Utc>,
    market_mode: MarketMode,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StatusResponse {
    service: String,
    environment: String,
    market_mode: MarketMode,
    started_at: chrono::DateTime<Utc>,
    timestamp: chrono::DateTime<Utc>,
    dependencies: Dependencies,
    todos: Vec<&'static str>,
}

#[derive(Serialize)]
struct Dependencies {
    database: &'static str,
    event_bus: &'static str,
    exchange_execution: &'static str,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let app_name = env::var("APP_NAME").unwrap_or_else(|_| "aegis-quant-api".to_string());
    let bind_addr = env::var("API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let environment = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());

    let state = AppState {
        app_name: app_name.clone(),
        started_at: Utc::now(),
        market_mode: MarketMode::Paper,
    };

    let app = Router::new()
        .route("/system/health", get(health))
        .route("/system/status", get(status))
        .with_state((state, environment))
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = bind_addr.parse().expect("invalid API_BIND_ADDR");
    info!(%addr, service = %app_name, "starting API server");
    info!("TODO: add authn/authz boundary before non-internal exposure");

    let listener = tokio::net::TcpListener::bind(addr)
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

async fn health(State((state, _environment)): State<(AppState, String)>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: state.app_name,
        timestamp: Utc::now(),
    })
}

async fn status(State((state, environment)): State<(AppState, String)>) -> Json<StatusResponse> {
    Json(StatusResponse {
        service: state.app_name,
        environment,
        market_mode: state.market_mode,
        started_at: state.started_at,
        timestamp: Utc::now(),
        dependencies: Dependencies {
            database: "not_connected",
            event_bus: "not_configured",
            exchange_execution: "disabled_for_mvp",
        },
        todos: vec![
            "auth boundary not implemented yet",
            "database connectivity probe not wired yet",
            "kill switch persistence not implemented yet",
        ],
    })
}
