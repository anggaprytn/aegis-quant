use std::time::Duration;

use aegis_core::MarketMode;
use api::{
    scheduled_research::{
        run_scheduled_research_tick, scheduled_research_runner_mode_from_env,
        ScheduledResearchRunnerMode,
    },
    AppConfig, AppState, StrategyRuntimeConfig,
};
use chrono::Utc;
use db::{connect_pool, ensure_system_state, DbConfig};
use market_ingest::MarketIngestConfig;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_tracing();

    let mode =
        scheduled_research_runner_mode_from_env().expect("invalid scheduled runner configuration");
    let interval_seconds = match mode {
        ScheduledResearchRunnerMode::Enabled { interval_seconds } => interval_seconds,
        ScheduledResearchRunnerMode::Disabled { sleep_seconds } => {
            idle_while_disabled(sleep_seconds).await;
            return;
        }
    };

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

    info!(
        enabled = true,
        interval_seconds, "starting scheduled research runner daemon"
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

        if wait_for_shutdown_or_sleep(Duration::from_secs(interval_seconds.max(1))).await {
            info!("received shutdown signal, stopping scheduled research runner daemon");
            break;
        }
    }
}

async fn idle_while_disabled(sleep_seconds: u64) {
    info!(
        enabled = false,
        sleep_seconds,
        no_jobs_processed = true,
        "scheduled research runner disabled; idling"
    );
    loop {
        if wait_for_shutdown_or_sleep(Duration::from_secs(sleep_seconds.max(1))).await {
            info!("received shutdown signal, stopping disabled scheduled research runner");
            break;
        }
        info!(
            enabled = false,
            sleep_seconds,
            no_jobs_processed = true,
            "scheduled research runner disabled; idling"
        );
    }
}

async fn wait_for_shutdown_or_sleep(duration: Duration) -> bool {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    warn!(error = %err, "failed to listen for ctrl-c");
                }
                true
            }
            _ = sigterm.recv() => true,
            _ = tokio::time::sleep(duration) => false,
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    warn!(error = %err, "failed to listen for ctrl-c");
                }
                true
            }
            _ = tokio::time::sleep(duration) => false,
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
