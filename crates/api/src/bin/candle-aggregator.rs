use std::{env, time::Duration as StdDuration};

use aegis_core::{
    aggregate_closed_1m_candles, candle_aggregation_start_time, CandleAggregationRun,
    CandleInterval, MarketDataSource,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use db::{
    connect_pool, get_closed_1m_candles_range, get_latest_closed_candle_time,
    insert_candle_aggregation_run, upsert_aggregated_candles, DbConfig,
};
use market_ingest::MarketIngestConfig;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let db_pool = connect_pool(&DbConfig {
        database_url,
        max_connections: env::var("DATABASE_MAX_CONNECTIONS")
            .ok()
            .map(|value| value.parse())
            .transpose()
            .context("invalid DATABASE_MAX_CONNECTIONS")?
            .unwrap_or(5),
    })
    .await
    .context("failed to connect to Postgres")?;
    let market_config = MarketIngestConfig::from_env().context("invalid market configuration")?;
    let config = CandleAggregatorConfig::from_env()?;

    info!(
        exchange = market_config.exchange.as_str(),
        symbols = ?market_config.symbols_as_strings(),
        targets = ?config.target_intervals.iter().map(|item| item.as_str()).collect::<Vec<_>>(),
        interval_seconds = config.interval_seconds,
        bootstrap_lookback_hours = config.bootstrap_lookback.num_hours(),
        overlap_minutes = config.overlap.num_minutes(),
        "starting candle aggregator worker"
    );

    loop {
        for symbol in &market_config.symbols {
            for target_interval in &config.target_intervals {
                if let Err(err) = aggregate_once(
                    &db_pool,
                    market_config.exchange,
                    symbol,
                    *target_interval,
                    config.bootstrap_lookback,
                    config.overlap,
                )
                .await
                {
                    error!(
                        symbol = %symbol,
                        target_interval = %target_interval.as_str(),
                        error = %err,
                        "candle aggregation tick failed"
                    );
                }
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c, stopping candle aggregator worker");
                break;
            }
            _ = tokio::time::sleep(StdDuration::from_secs(config.interval_seconds.max(1))) => {}
        }
    }

    Ok(())
}

struct CandleAggregatorConfig {
    target_intervals: Vec<CandleInterval>,
    interval_seconds: u64,
    bootstrap_lookback: Duration,
    overlap: Duration,
}

impl CandleAggregatorConfig {
    fn from_env() -> Result<Self> {
        let target_intervals = env::var("CANDLE_AGGREGATOR_TARGET_INTERVALS")
            .unwrap_or_else(|_| "5m,15m,1h".to_string())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.parse::<CandleInterval>())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let target_intervals = if target_intervals.is_empty() {
            vec![
                CandleInterval::FiveMinutes,
                CandleInterval::FifteenMinutes,
                CandleInterval::OneHour,
            ]
        } else {
            target_intervals
        };
        for interval in &target_intervals {
            if !interval.is_aggregated_from_one_minute() {
                anyhow::bail!(
                    "CANDLE_AGGREGATOR_TARGET_INTERVALS must contain derived intervals only"
                );
            }
        }

        Ok(Self {
            target_intervals,
            interval_seconds: env::var("CANDLE_AGGREGATOR_INTERVAL_SECONDS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("invalid CANDLE_AGGREGATOR_INTERVAL_SECONDS")?,
            bootstrap_lookback: Duration::hours(
                env::var("CANDLE_AGGREGATOR_BOOTSTRAP_LOOKBACK_HOURS")
                    .unwrap_or_else(|_| "24".to_string())
                    .parse()
                    .context("invalid CANDLE_AGGREGATOR_BOOTSTRAP_LOOKBACK_HOURS")?,
            ),
            overlap: Duration::minutes(
                env::var("CANDLE_AGGREGATOR_OVERLAP_MINUTES")
                    .unwrap_or_else(|_| "120".to_string())
                    .parse()
                    .context("invalid CANDLE_AGGREGATOR_OVERLAP_MINUTES")?,
            ),
        })
    }
}

async fn aggregate_once(
    pool: &db::PgPool,
    exchange: MarketDataSource,
    symbol: &aegis_core::Symbol,
    target_interval: CandleInterval,
    bootstrap_lookback: Duration,
    overlap: Duration,
) -> Result<()> {
    let started_at = Utc::now();
    let latest_source =
        get_latest_closed_candle_time(pool, exchange, symbol, CandleInterval::OneMinute).await?;
    let latest_target =
        get_latest_closed_candle_time(pool, exchange, symbol, target_interval).await?;
    let start_time =
        candle_aggregation_start_time(latest_target, started_at, bootstrap_lookback, overlap);

    let Some(end_time) = latest_source else {
        persist_run(
            pool,
            symbol.as_str(),
            target_interval,
            started_at,
            "COMPLETED",
            0,
            0,
            0,
            0,
            None,
            latest_target,
            None,
        )
        .await?;
        info!(
            symbol = %symbol,
            target_interval = %target_interval.as_str(),
            source_candles = 0,
            inserted = 0,
            updated = 0,
            skipped_incomplete = 0,
            latest_derived_candle_time = ?latest_target,
            "candle aggregation tick completed without source candles"
        );
        return Ok(());
    };

    if end_time <= start_time {
        warn!(
            symbol = %symbol,
            target_interval = %target_interval.as_str(),
            start_time = %start_time,
            end_time = %end_time,
            "skipping candle aggregation because window is empty"
        );
        return Ok(());
    }

    let source_candles =
        get_closed_1m_candles_range(pool, exchange, symbol, start_time, end_time).await?;
    let aggregated = aggregate_closed_1m_candles(&source_candles, target_interval);
    let upsert = upsert_aggregated_candles(pool, &aggregated.candles).await?;
    let latest_target_after =
        get_latest_closed_candle_time(pool, exchange, symbol, target_interval).await?;

    persist_run(
        pool,
        symbol.as_str(),
        target_interval,
        started_at,
        "COMPLETED",
        i32::try_from(source_candles.len()).unwrap_or(i32::MAX),
        upsert.inserted_candles,
        upsert.updated_candles,
        aggregated.skipped_incomplete_buckets,
        Some(end_time),
        latest_target_after,
        None,
    )
    .await?;

    info!(
        symbol = %symbol,
        target_interval = %target_interval.as_str(),
        source_candles = source_candles.len(),
        inserted = upsert.inserted_candles,
        updated = upsert.updated_candles,
        skipped_incomplete = aggregated.skipped_incomplete_buckets,
        latest_derived_candle_time = ?latest_target_after,
        "candle aggregation tick completed"
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn persist_run(
    pool: &db::PgPool,
    symbol: &str,
    target_interval: CandleInterval,
    started_at: chrono::DateTime<Utc>,
    status: &str,
    source_candles: i32,
    inserted: i32,
    updated: i32,
    skipped_incomplete: i32,
    latest_source_closed_time: Option<chrono::DateTime<Utc>>,
    latest_target_closed_time: Option<chrono::DateTime<Utc>>,
    error: Option<String>,
) -> Result<()> {
    insert_candle_aggregation_run(
        pool,
        &CandleAggregationRun {
            id: Uuid::new_v4(),
            symbol: symbol.to_string(),
            source_interval: CandleInterval::OneMinute.as_str().to_string(),
            target_interval: target_interval.as_str().to_string(),
            started_at,
            completed_at: Some(Utc::now()),
            status: status.to_string(),
            source_candles,
            inserted,
            updated,
            skipped_incomplete,
            latest_source_closed_time,
            latest_target_closed_time,
            error,
        },
    )
    .await?;
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,sqlx=warn".into());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
