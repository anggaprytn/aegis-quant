use std::time::Duration;

use aegis_core::MarketMode;
use api::{
    testnet_shadow_runner::{
        load_testnet_shadow_runner_snapshot, run_shadow_runner_tick, RunnerTickMode,
    },
    AppConfig, AppState, StrategyRuntimeConfig,
};
use chrono::Utc;
use db::{connect_pool, ensure_system_state, DbConfig, StateActor};
use market_ingest::MarketIngestConfig;
use tracing::{info, warn};
use uuid::Uuid;

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

    let state = AppState {
        config,
        db_pool,
        started_at: Utc::now(),
        market_mode: MarketMode::Paper,
        market_config: MarketIngestConfig::from_env().expect("invalid market ingest configuration"),
        strategy_runtime: StrategyRuntimeConfig::from_env()
            .expect("invalid strategy runtime configuration"),
    };
    let actor = StateActor::system("testnet-shadow-runner-daemon");

    info!("starting testnet shadow runner daemon");
    loop {
        let snapshot = load_testnet_shadow_runner_snapshot(&state)
            .await
            .expect("runner snapshot should load");
        let sleep_for = Duration::from_secs(snapshot.config.interval_seconds.max(1) as u64);

        let tick = run_shadow_runner_tick(
            &state,
            Some(&actor),
            Some(Uuid::new_v4()),
            RunnerTickMode::Scheduled,
        )
        .await;
        match tick {
            Ok(result) => {
                info!(
                    status = result.status.as_str(),
                    scheduled = result.scheduled,
                    attempted_runs = result.attempted_runs,
                    completed_runs = result.completed_runs,
                    failed_runs = result.failed_runs,
                    message = result.message.as_deref().unwrap_or(""),
                    "testnet shadow runner tick completed"
                );
            }
            Err(err) => {
                warn!(error = %err, "testnet shadow runner tick failed");
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c, stopping testnet shadow runner daemon");
                break;
            }
            _ = tokio::time::sleep(sleep_for) => {}
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
