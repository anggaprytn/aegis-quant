use std::time::Duration;

use aegis_core::MarketMode;
use api::{
    scheduled_research::{run_scheduled_research_tick, runner_interval_from_env},
    AppConfig, AppState, StrategyRuntimeConfig,
};
use chrono::Utc;
use db::{connect_pool, ensure_system_state, DbConfig};
use market_ingest::MarketIngestConfig;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_tracing();

    let enabled = std::env::var("SCHEDULED_RESEARCH_RUNNER_ENABLED")
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false);
    if !enabled {
        info!(
            "scheduled research runner disabled; set SCHEDULED_RESEARCH_RUNNER_ENABLED=true to run"
        );
        return;
    }

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

    let state = AppState {
        config,
        db_pool,
        started_at: Utc::now(),
        market_mode: MarketMode::Paper,
        market_config: MarketIngestConfig::from_env().expect("invalid market ingest configuration"),
        strategy_runtime: StrategyRuntimeConfig::from_env()
            .expect("invalid strategy runtime configuration"),
    };
    let interval_seconds = runner_interval_from_env().expect("invalid scheduled runner interval");

    info!(
        interval_seconds,
        "starting scheduled research runner daemon"
    );
    loop {
        match run_scheduled_research_tick(&state, 100).await {
            Ok(result) => {
                info!(
                    attempted_jobs = result.attempted_jobs,
                    completed_runs = result.completed_runs,
                    failed_runs = result.failed_runs,
                    skipped_runs = result.skipped_runs,
                    "scheduled research runner tick completed"
                );
            }
            Err(err) => {
                warn!(error = %err, "scheduled research runner tick failed");
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c, stopping scheduled research runner daemon");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(interval_seconds.max(1))) => {}
        }
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,sqlx=warn".into());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
