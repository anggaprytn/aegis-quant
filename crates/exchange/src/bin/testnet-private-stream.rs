use std::{env, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use db::{
    connect_pool, insert_exchange_private_stream_event, update_exchange_testnet_order_status,
    upsert_exchange_private_stream_state, DbConfig, ExchangePrivateStreamEventRecord,
    ExchangePrivateStreamStateRecord,
};
use exchange::{
    build_private_stream_state, hash_listen_key,
    local_testnet_order_status_from_private_execution_report, parse_binance_private_stream_event,
    private_stream_is_stale, BinanceSpotTestnetAdapter, BinanceSpotTestnetConfig,
};
use futures_util::StreamExt;
use telemetry::telemetry;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,tungstenite=warn")),
        )
        .init();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let db_pool = connect_pool(&DbConfig::new(database_url)).await?;
    let adapter = BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig::from_env());
    let stale_threshold = Duration::from_secs(read_env_u64(
        "BINANCE_TESTNET_PRIVATE_STREAM_STALE_THRESHOLD_SECONDS",
        90,
    ));
    let keepalive_interval = Duration::from_secs(read_env_u64(
        "BINANCE_TESTNET_PRIVATE_STREAM_KEEPALIVE_SECONDS",
        1_800,
    ));
    let reconnect_delay = Duration::from_secs(read_env_u64(
        "BINANCE_TESTNET_PRIVATE_STREAM_RECONNECT_DELAY_SECONDS",
        5,
    ));

    let mut reconnect_count = 0;
    loop {
        let listen_key = match adapter.create_listen_key().await {
            Ok(value) => value,
            Err(err) => {
                telemetry().inc_exchange_private_stream_error("testnet", "listen_key_create");
                persist_state(
                    &db_pool,
                    reconnect_count,
                    build_private_stream_state(
                        aegis_core::ExchangePrivateStreamStatus::Error,
                        None,
                        None,
                        None,
                        Some(err.to_string()),
                        reconnect_count,
                    ),
                )
                .await?;
                error!(error = %err, "failed to create testnet listen key");
                sleep(reconnect_delay).await;
                reconnect_count += 1;
                continue;
            }
        };

        let listen_key_hash = hash_listen_key(&listen_key.listen_key);
        persist_state(
            &db_pool,
            reconnect_count,
            build_private_stream_state(
                aegis_core::ExchangePrivateStreamStatus::Connecting,
                Some(listen_key_hash.clone()),
                None,
                None,
                None,
                reconnect_count,
            ),
        )
        .await?;

        let ws_url = adapter.build_user_stream_url(&listen_key.listen_key)?;
        match connect_async(ws_url.as_str()).await {
            Ok((stream, _)) => {
                info!("connected to Binance Spot Testnet private stream");
                let connected_at = Utc::now();
                persist_state(
                    &db_pool,
                    reconnect_count,
                    build_private_stream_state(
                        aegis_core::ExchangePrivateStreamStatus::Connected,
                        Some(listen_key_hash.clone()),
                        Some(connected_at),
                        None,
                        None,
                        reconnect_count,
                    ),
                )
                .await?;

                if let Err(err) = process_stream(
                    &db_pool,
                    &adapter,
                    stream,
                    &listen_key.listen_key,
                    listen_key_hash.clone(),
                    reconnect_count,
                    keepalive_interval,
                    stale_threshold,
                    connected_at,
                )
                .await
                {
                    telemetry().inc_exchange_private_stream_error("testnet", "runtime");
                    warn!(error = %err, "private stream loop exited with error");
                }
            }
            Err(err) => {
                telemetry().inc_exchange_private_stream_error("testnet", "connect");
                error!(error = %err, "failed to connect to testnet private stream websocket");
                persist_state(
                    &db_pool,
                    reconnect_count + 1,
                    build_private_stream_state(
                        aegis_core::ExchangePrivateStreamStatus::Error,
                        Some(listen_key_hash.clone()),
                        None,
                        None,
                        Some(err.to_string()),
                        reconnect_count + 1,
                    ),
                )
                .await?;
            }
        }

        let _ = adapter.close_listen_key(&listen_key.listen_key).await;
        reconnect_count += 1;
        sleep(reconnect_delay).await;
    }
}

async fn process_stream(
    db_pool: &db::PgPool,
    adapter: &BinanceSpotTestnetAdapter,
    mut stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    listen_key: &str,
    listen_key_hash: String,
    reconnect_count: i32,
    keepalive_every: Duration,
    stale_threshold: Duration,
    connected_at: chrono::DateTime<Utc>,
) -> Result<()> {
    let mut keepalive = interval(keepalive_every);
    let mut stale_check = interval(Duration::from_secs(5));
    let mut last_event_at = None;

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                if let Err(err) = adapter.keepalive_listen_key(listen_key).await {
                    telemetry().inc_exchange_private_stream_error("testnet", "keepalive");
                    persist_state(
                        db_pool,
                        reconnect_count + 1,
                        build_private_stream_state(
                            aegis_core::ExchangePrivateStreamStatus::Error,
                            Some(listen_key_hash.clone()),
                            Some(connected_at),
                            last_event_at,
                            Some(err.to_string()),
                            reconnect_count + 1,
                        ),
                    ).await?;
                    anyhow::bail!("listen key keepalive failed: {err}");
                }
            }
            _ = stale_check.tick() => {
                let state = build_private_stream_state(
                    aegis_core::ExchangePrivateStreamStatus::Connected,
                    Some(listen_key_hash.clone()),
                    Some(connected_at),
                    last_event_at,
                    None,
                    reconnect_count,
                );
                if private_stream_is_stale(&state, Utc::now(), stale_threshold) {
                    telemetry().inc_exchange_private_stream_error("testnet", "stale");
                    persist_state(
                        db_pool,
                        reconnect_count + 1,
                        build_private_stream_state(
                            aegis_core::ExchangePrivateStreamStatus::Stale,
                            Some(listen_key_hash.clone()),
                            Some(connected_at),
                            last_event_at,
                            Some("private stream stale threshold exceeded".to_string()),
                            reconnect_count + 1,
                        ),
                    ).await?;
                    anyhow::bail!("private stream became stale");
                }
            }
            message = stream.next() => {
                let message = match message {
                    Some(message) => message,
                    None => {
                        persist_state(
                            db_pool,
                            reconnect_count + 1,
                            build_private_stream_state(
                                aegis_core::ExchangePrivateStreamStatus::Disconnected,
                                Some(listen_key_hash.clone()),
                                Some(connected_at),
                                last_event_at,
                                Some("websocket closed".to_string()),
                                reconnect_count + 1,
                            ),
                        ).await?;
                        anyhow::bail!("websocket closed");
                    }
                };

                match message? {
                    Message::Text(text) => {
                        handle_message(db_pool, &text, reconnect_count, &listen_key_hash, connected_at, &mut last_event_at).await?;
                    }
                    Message::Binary(data) => {
                        let text = String::from_utf8(data.to_vec()).context("binary websocket frame was not utf8")?;
                        handle_message(db_pool, &text, reconnect_count, &listen_key_hash, connected_at, &mut last_event_at).await?;
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Close(_) => {
                        persist_state(
                            db_pool,
                            reconnect_count + 1,
                            build_private_stream_state(
                                aegis_core::ExchangePrivateStreamStatus::Disconnected,
                                Some(listen_key_hash.clone()),
                                Some(connected_at),
                                last_event_at,
                                Some("websocket close frame received".to_string()),
                                reconnect_count + 1,
                            ),
                        ).await?;
                        anyhow::bail!("websocket close frame received");
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn handle_message(
    db_pool: &db::PgPool,
    text: &str,
    reconnect_count: i32,
    listen_key_hash: &str,
    connected_at: chrono::DateTime<Utc>,
    last_event_at: &mut Option<chrono::DateTime<Utc>>,
) -> Result<()> {
    let payload: serde_json::Value =
        serde_json::from_str(text).context("parse websocket payload")?;
    let processed = parse_binance_private_stream_event(&payload, Utc::now())?;
    let event = processed.event;
    telemetry().inc_exchange_private_stream_event("testnet", &event.event_type);

    insert_exchange_private_stream_event(
        db_pool,
        &ExchangePrivateStreamEventRecord {
            id: Uuid::new_v4(),
            exchange: event.exchange.as_str().to_string(),
            environment: event.environment.as_str().to_string(),
            event_type: event.event_type.clone(),
            symbol: event.symbol.clone(),
            client_order_id: event.client_order_id.clone(),
            exchange_order_id: event.exchange_order_id.clone(),
            execution_type: event.execution_type.map(|value| value.as_str().to_string()),
            order_status: event.order_status.map(|value| value.as_str().to_string()),
            payload: event.raw_payload.clone(),
            event_time: event.event_time,
            received_at: event.received_at,
            correlation_id: None,
        },
    )
    .await?;

    if let Some(report) = processed.execution_report {
        let _ = update_exchange_testnet_order_status(
            db_pool,
            &report.client_order_id,
            report.exchange_order_id.as_deref(),
            local_testnet_order_status_from_private_execution_report(&report),
            &report.raw_payload,
        )
        .await?;
    }

    *last_event_at = Some(event.event_time);
    telemetry().set_exchange_private_stream_status("testnet", "CONNECTED");
    telemetry().set_exchange_private_stream_last_event_age_seconds("testnet", 0.0);
    persist_state(
        db_pool,
        reconnect_count,
        build_private_stream_state(
            aegis_core::ExchangePrivateStreamStatus::Connected,
            Some(listen_key_hash.to_string()),
            Some(connected_at),
            *last_event_at,
            None,
            reconnect_count,
        ),
    )
    .await?;

    Ok(())
}

async fn persist_state(
    db_pool: &db::PgPool,
    reconnect_count: i32,
    state: aegis_core::ExchangePrivateStreamState,
) -> Result<()> {
    telemetry().set_exchange_private_stream_status("testnet", state.status.as_str());
    if let Some(last_event_at) = state.last_event_at {
        let age_seconds = Utc::now()
            .signed_duration_since(last_event_at)
            .to_std()
            .map(|age| age.as_secs_f64())
            .unwrap_or(0.0);
        telemetry().set_exchange_private_stream_last_event_age_seconds("testnet", age_seconds);
    }

    upsert_exchange_private_stream_state(
        db_pool,
        &ExchangePrivateStreamStateRecord {
            exchange: state.exchange.as_str().to_string(),
            environment: state.environment.as_str().to_string(),
            status: state.status.as_str().to_string(),
            listen_key_hash: state.listen_key_hash,
            connected_at: state.connected_at,
            last_event_at: state.last_event_at,
            last_error: state.last_error,
            reconnect_count,
            updated_at: state.updated_at,
        },
    )
    .await?;

    Ok(())
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}
