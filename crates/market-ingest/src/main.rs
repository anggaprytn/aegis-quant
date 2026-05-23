use db::{connect_pool, ensure_system_state, DbConfig};
use market_ingest::{MarketIngestConfig, MarketIngestService};

#[tokio::main]
async fn main() {
    init_tracing();

    let config = MarketIngestConfig::from_env().expect("invalid market ingest configuration");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let database_max_connections = std::env::var("DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(5);

    let pool = connect_pool(&DbConfig {
        database_url,
        max_connections: database_max_connections,
    })
    .await
    .expect("failed to connect to Postgres");

    ensure_system_state(&pool)
        .await
        .expect("failed to initialize persistent system state");

    let mut service = MarketIngestService::new(pool, config);
    service
        .run_loop()
        .await
        .expect("market ingest service stopped unexpectedly");
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,market_ingest=info,tungstenite=warn".into());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
