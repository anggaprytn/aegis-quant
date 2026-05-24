use std::{collections::HashMap, env, time::Duration};

use aegis_core::{
    Candle, CandleBackfillProgress, CandleBackfillRequest, CandleBackfillResult, CandleInterval,
    DataFreshnessStatus, FeedStatus, MarketDataSource, MarketTick, MarketTrade, Symbol,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use db::{
    candle_backfill_result_from_record, complete_candle_backfill_run, fail_candle_backfill_run,
    insert_candle_backfill_run, process_market_trade, update_candle_backfill_progress,
    upsert_candles_batch, upsert_market_feed_status, PgPool,
};
use events::{EventPublisher, PostgresEventPublisher, SystemEventType};
use futures_util::StreamExt;
use reqwest::Url;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketIngestConfig {
    pub exchange: MarketDataSource,
    pub symbols: Vec<Symbol>,
    pub stale_threshold: Duration,
    pub binance_ws_base_url: String,
    pub binance_rest_base_url: String,
}

impl MarketIngestConfig {
    pub fn from_env() -> Result<Self> {
        let exchange = env::var("MARKET_EXCHANGE")
            .unwrap_or_else(|_| "binance".to_string())
            .parse()?;
        let symbols = env::var("MARKET_SYMBOLS")
            .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
            .split(',')
            .map(Symbol::new)
            .collect::<Result<Vec<_>, _>>()?;
        let stale_threshold_seconds = env::var("MARKET_STALE_THRESHOLD_SECONDS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u64>()?;
        let binance_ws_base_url = env::var("BINANCE_WS_BASE_URL")
            .unwrap_or_else(|_| "wss://stream.binance.com:9443".to_string());
        let binance_rest_base_url = env::var("BINANCE_REST_BASE_URL")
            .unwrap_or_else(|_| "https://api.binance.com".to_string());

        Ok(Self {
            exchange,
            symbols,
            stale_threshold: Duration::from_secs(stale_threshold_seconds),
            binance_ws_base_url,
            binance_rest_base_url,
        })
    }

    pub fn symbols_as_strings(&self) -> Vec<String> {
        self.symbols
            .iter()
            .map(|symbol| symbol.as_str().to_string())
            .collect()
    }
}

#[derive(Debug, Error)]
pub enum CandleBuilderError {
    #[error("market trade price must be greater than zero")]
    InvalidPrice,
    #[error("market trade quantity must be greater than zero")]
    InvalidQuantity,
    #[error("out-of-order trade rejected: {trade_time} is before active candle open time {active_open_time}")]
    OutOfOrderTrade {
        trade_time: DateTime<Utc>,
        active_open_time: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
pub struct CandleUpdate {
    pub active: Candle,
    pub closed: Option<Candle>,
}

#[derive(Debug, Clone)]
pub struct CandleBuilder {
    exchange: MarketDataSource,
    symbol: Symbol,
    interval: CandleInterval,
    active: Option<Candle>,
}

impl CandleBuilder {
    pub fn new(exchange: MarketDataSource, symbol: Symbol, interval: CandleInterval) -> Self {
        Self {
            exchange,
            symbol,
            interval,
            active: None,
        }
    }

    pub fn apply_trade(
        &mut self,
        trade: &MarketTrade,
    ) -> std::result::Result<CandleUpdate, CandleBuilderError> {
        if trade.price <= Decimal::ZERO {
            return Err(CandleBuilderError::InvalidPrice);
        }
        if trade.quantity <= Decimal::ZERO {
            return Err(CandleBuilderError::InvalidQuantity);
        }

        let open_time = floor_time(trade.trade_time, self.interval);
        let close_time = open_time + self.interval.duration();
        let quote_volume = trade.price * trade.quantity;

        match self.active.take() {
            None => {
                let candle = new_candle(
                    self.exchange,
                    self.symbol.clone(),
                    self.interval,
                    open_time,
                    close_time,
                    trade,
                    quote_volume,
                );
                self.active = Some(candle.clone());
                Ok(CandleUpdate {
                    active: candle,
                    closed: None,
                })
            }
            Some(mut active) => {
                if trade.trade_time < active.open_time {
                    self.active = Some(active.clone());
                    return Err(CandleBuilderError::OutOfOrderTrade {
                        trade_time: trade.trade_time,
                        active_open_time: active.open_time,
                    });
                }

                if open_time == active.open_time {
                    update_candle(&mut active, trade, quote_volume);
                    self.active = Some(active.clone());
                    return Ok(CandleUpdate {
                        active,
                        closed: None,
                    });
                }

                active.is_closed = true;
                active.updated_at = trade.trade_time;

                let next = new_candle(
                    self.exchange,
                    self.symbol.clone(),
                    self.interval,
                    open_time,
                    close_time,
                    trade,
                    quote_volume,
                );
                self.active = Some(next.clone());

                Ok(CandleUpdate {
                    active: next,
                    closed: Some(active),
                })
            }
        }
    }

    pub fn active(&self) -> Option<&Candle> {
        self.active.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct BinanceTradeStreamClient {
    base_url: String,
    symbols: Vec<Symbol>,
}

impl BinanceTradeStreamClient {
    pub fn new(base_url: impl Into<String>, symbols: Vec<Symbol>) -> Self {
        Self {
            base_url: base_url.into(),
            symbols,
        }
    }

    pub fn combined_stream_url(&self) -> String {
        let streams = self
            .symbols
            .iter()
            .map(|symbol| format!("{}@trade", symbol.as_str().to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join("/");

        format!(
            "{}/stream?streams={streams}",
            self.base_url.trim_end_matches('/')
        )
    }

    pub async fn connect(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let (stream, _) = connect_async(self.combined_stream_url()).await?;
        Ok(stream)
    }
}

#[derive(Debug)]
pub struct MarketIngestService {
    pool: PgPool,
    config: MarketIngestConfig,
    reconnect_count: i32,
    builders: HashMap<String, CandleBuilder>,
    last_seen_at: HashMap<String, DateTime<Utc>>,
}

impl MarketIngestService {
    pub fn new(pool: PgPool, config: MarketIngestConfig) -> Self {
        let builders = config
            .symbols
            .iter()
            .cloned()
            .map(|symbol| {
                let builder =
                    CandleBuilder::new(config.exchange, symbol.clone(), CandleInterval::OneMinute);
                (symbol.as_str().to_string(), builder)
            })
            .collect();

        Self {
            pool,
            config,
            reconnect_count: 0,
            builders,
            last_seen_at: HashMap::new(),
        }
    }

    pub async fn mark_connected(&mut self) -> Result<()> {
        let publisher = PostgresEventPublisher::new(self.pool.clone());
        let connected_at = Utc::now();

        for symbol in &self.config.symbols {
            self.last_seen_at
                .insert(symbol.as_str().to_string(), connected_at);
            upsert_market_feed_status(
                &self.pool,
                self.config.exchange,
                symbol,
                FeedStatus::Connected,
                DataFreshnessStatus::Unknown,
                Some(connected_at),
                None,
                self.reconnect_count,
            )
            .await?;
            publisher
                .publish(SystemEventType::MarketFeedConnected.into_event(
                    Uuid::new_v4(),
                    "market-ingest.binance",
                    serde_json::json!({
                        "exchange": self.config.exchange.as_str(),
                        "symbol": symbol.as_str(),
                        "status": FeedStatus::Connected.as_str(),
                    }),
                ))
                .await?;
        }

        Ok(())
    }

    pub async fn mark_disconnected(&self, last_error: Option<&str>) -> Result<()> {
        let publisher = PostgresEventPublisher::new(self.pool.clone());

        for symbol in &self.config.symbols {
            upsert_market_feed_status(
                &self.pool,
                self.config.exchange,
                symbol,
                FeedStatus::Disconnected,
                DataFreshnessStatus::Unknown,
                None,
                last_error,
                self.reconnect_count,
            )
            .await?;
            publisher
                .publish(SystemEventType::MarketFeedDisconnected.into_event(
                    Uuid::new_v4(),
                    "market-ingest.binance",
                    serde_json::json!({
                        "exchange": self.config.exchange.as_str(),
                        "symbol": symbol.as_str(),
                        "status": FeedStatus::Disconnected.as_str(),
                        "last_error": last_error,
                    }),
                ))
                .await?;
        }

        Ok(())
    }

    pub async fn mark_stale_feeds(&self, now: DateTime<Utc>) -> Result<Vec<Symbol>> {
        let mut stale_symbols = Vec::new();
        let publisher = PostgresEventPublisher::new(self.pool.clone());

        for symbol in &self.config.symbols {
            let Some(last_seen_at) = self.last_seen_at.get(symbol.as_str()) else {
                continue;
            };

            let age = now.signed_duration_since(*last_seen_at);
            if age > chrono_duration(self.config.stale_threshold)? {
                upsert_market_feed_status(
                    &self.pool,
                    self.config.exchange,
                    symbol,
                    FeedStatus::Stale,
                    DataFreshnessStatus::Stale,
                    Some(*last_seen_at),
                    Some("market data feed exceeded stale threshold"),
                    self.reconnect_count,
                )
                .await?;
                publisher
                    .publish(SystemEventType::MarketFeedStale.into_event(
                        Uuid::new_v4(),
                        "market-ingest.binance",
                        serde_json::json!({
                            "exchange": self.config.exchange.as_str(),
                            "symbol": symbol.as_str(),
                            "status": FeedStatus::Stale.as_str(),
                            "last_event_at": last_seen_at,
                            "stale_threshold_seconds": self.config.stale_threshold.as_secs(),
                        }),
                    ))
                    .await?;
                stale_symbols.push(symbol.clone());
            }
        }

        Ok(stale_symbols)
    }

    pub async fn handle_trade(&mut self, trade: MarketTrade) -> Result<()> {
        self.last_seen_at
            .insert(trade.symbol.as_str().to_string(), trade.received_at);
        let tick = MarketTick {
            id: Uuid::new_v4(),
            exchange: trade.exchange,
            symbol: trade.symbol.clone(),
            price: trade.price,
            quantity: trade.quantity,
            trade_time: trade.trade_time,
            received_at: trade.received_at,
            raw_payload: trade.raw_payload.clone(),
        };

        let builder = self
            .builders
            .get_mut(trade.symbol.as_str())
            .ok_or_else(|| anyhow!("unsupported symbol {}", trade.symbol.as_str()))?;
        let update = builder.apply_trade(&trade)?;

        process_market_trade(
            &self.pool,
            "market-ingest.binance",
            &tick,
            &update.active,
            update.closed.as_ref(),
            self.reconnect_count,
        )
        .await?;

        Ok(())
    }

    pub async fn run_loop(&mut self) -> Result<()> {
        loop {
            let client = BinanceTradeStreamClient::new(
                self.config.binance_ws_base_url.clone(),
                self.config.symbols.clone(),
            );

            self.mark_connected().await?;
            let stream_url = client.combined_stream_url();
            info!(%stream_url, reconnect_count = self.reconnect_count, "connecting market feed");
            let run_result = self.run_single_connection(&client).await;

            match run_result {
                Ok(()) => {
                    warn!("market feed ended without explicit error");
                    self.mark_disconnected(None).await?;
                }
                Err(err) => {
                    warn!(error = %err, "market feed disconnected");
                    self.mark_disconnected(Some(&err.to_string())).await?;
                }
            }

            self.reconnect_count += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn run_single_connection(&mut self, client: &BinanceTradeStreamClient) -> Result<()> {
        let stream = client.connect().await?;
        let (_, mut read) = stream.split();
        let mut stale_emitted = false;

        loop {
            let next_message = tokio::time::timeout(self.config.stale_threshold, read.next()).await;
            match next_message {
                Ok(Some(message)) => {
                    let message = message?;
                    if let Message::Text(text) = message {
                        let trade = parse_binance_trade_message(&text)?;
                        self.handle_trade(trade).await?;
                        stale_emitted = false;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    if !stale_emitted {
                        self.mark_stale_feeds(Utc::now()).await?;
                        stale_emitted = true;
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct BinanceCombinedTradeMessage {
    data: BinanceTradePayload,
}

#[derive(Debug, Deserialize)]
struct BinanceTradePayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

pub fn parse_binance_trade_message(message: &str) -> Result<MarketTrade> {
    let raw_payload: Value = serde_json::from_str(message)?;
    let parsed: BinanceCombinedTradeMessage = serde_json::from_value(raw_payload.clone())?;
    let symbol = Symbol::new(parsed.data.symbol)?;
    let trade_time = Utc
        .timestamp_millis_opt(parsed.data.trade_time_ms)
        .single()
        .ok_or_else(|| anyhow!("invalid Binance trade timestamp"))?;

    Ok(MarketTrade {
        trade_id: parsed.data.trade_id.to_string(),
        exchange: MarketDataSource::Binance,
        symbol,
        price: parsed.data.price.parse()?,
        quantity: parsed.data.quantity.parse()?,
        trade_time,
        received_at: Utc::now(),
        is_buyer_maker: Some(parsed.data.is_buyer_maker),
        raw_payload: Some(raw_payload),
    })
}

#[derive(Debug, Clone)]
pub struct BinanceRestKlineClient {
    base_url: Url,
    http: reqwest::Client,
}

impl BinanceRestKlineClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(base_url)?,
            http: reqwest::Client::new(),
        })
    }

    pub async fn fetch_klines(
        &self,
        symbol: &str,
        interval: CandleInterval,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: u16,
    ) -> Result<Vec<BinanceKlineRow>> {
        let url = self.base_url.join("/api/v3/klines")?;
        let response = self
            .http
            .get(url)
            .query(&[
                ("symbol", symbol.to_string()),
                ("interval", interval.as_str().to_string()),
                ("startTime", start_time.timestamp_millis().to_string()),
                ("endTime", end_time.timestamp_millis().to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json::<Vec<BinanceKlineRow>>().await?)
    }
}

#[derive(Debug, Clone)]
pub struct HistoricalCandleBackfillService {
    pool: PgPool,
    source: String,
    client: BinanceRestKlineClient,
}

impl HistoricalCandleBackfillService {
    pub fn new(
        pool: PgPool,
        source: impl Into<String>,
        binance_rest_base_url: &str,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            source: source.into(),
            client: BinanceRestKlineClient::new(binance_rest_base_url)?,
        })
    }

    pub async fn run(&self, request: CandleBackfillRequest) -> Result<CandleBackfillResult> {
        request.validate()?;
        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let run_id = Uuid::new_v4();
        let created_at = Utc::now();
        let event_publisher = PostgresEventPublisher::new(self.pool.clone());
        let interval = request.parsed_interval()?;
        let symbol = request.normalized_symbol()?;
        let pages = plan_backfill_pages(
            request.start_time,
            request.end_time,
            interval,
            request.effective_limit_per_request(),
        )?;

        insert_candle_backfill_run(
            &self.pool,
            run_id,
            &request,
            correlation_id,
            created_at,
            serde_json::to_value(&request)?,
        )
        .await?;

        let execution = async {
            event_publisher
                .publish(SystemEventType::MarketBackfillStarted.into_event(
                    correlation_id,
                    self.source.clone(),
                    serde_json::json!({
                        "run_id": run_id,
                        "exchange": request.exchange.as_str(),
                        "symbol": symbol.as_str(),
                        "interval": interval.as_str(),
                        "start_time": request.start_time,
                        "end_time": request.end_time,
                        "requested_candles_estimate": request.requested_candles_estimate()?,
                    }),
                ))
                .await?;

            for (index, page) in pages.iter().enumerate() {
                let rows = self
                    .client
                    .fetch_klines(
                        symbol.as_str(),
                        interval,
                        page.start_time,
                        page.end_time,
                        request.effective_limit_per_request(),
                    )
                    .await?;

                let mut candles = Vec::new();
                let now = Utc::now();
                for row in &rows {
                    if let Some(candle) = binance_kline_row_to_closed_candle(
                        row,
                        request.exchange,
                        &symbol,
                        interval,
                        now,
                    )? {
                        candles.push(candle);
                    }
                }

                let upsert = upsert_candles_batch(&self.pool, &candles).await?;
                let progress = CandleBackfillProgress {
                    run_id,
                    page: i32::try_from(index + 1).unwrap_or(i32::MAX),
                    request_start_time: page.start_time,
                    request_end_time: page.end_time,
                    fetched_candles: i32::try_from(candles.len()).unwrap_or(i32::MAX),
                    inserted_candles: upsert.inserted_candles,
                    updated_candles: upsert.updated_candles,
                    skipped_candles: upsert.skipped_candles,
                };
                update_candle_backfill_progress(&self.pool, run_id, &progress).await?;

                event_publisher
                    .publish(SystemEventType::MarketBackfillPageFetched.into_event(
                        correlation_id,
                        self.source.clone(),
                        serde_json::json!({
                            "run_id": run_id,
                            "page": progress.page,
                            "page_start_time": progress.request_start_time,
                            "page_end_time": progress.request_end_time,
                            "fetched_candles": progress.fetched_candles,
                            "inserted_candles": progress.inserted_candles,
                            "updated_candles": progress.updated_candles,
                            "skipped_candles": progress.skipped_candles,
                        }),
                    ))
                    .await?;
            }

            let completed = complete_candle_backfill_run(&self.pool, run_id, Utc::now()).await?;
            let result = candle_backfill_result_from_record(&completed)?;
            event_publisher
                .publish(SystemEventType::MarketBackfillCompleted.into_event(
                    correlation_id,
                    self.source.clone(),
                    serde_json::json!({
                        "run_id": result.run_id,
                        "exchange": result.exchange.as_str(),
                        "symbol": result.symbol,
                        "interval": result.interval,
                        "fetched_candles": result.fetched_candles,
                        "inserted_candles": result.inserted_candles,
                        "updated_candles": result.updated_candles,
                        "skipped_candles": result.skipped_candles,
                    }),
                ))
                .await?;
            Ok::<CandleBackfillResult, anyhow::Error>(result)
        }
        .await;

        match execution {
            Ok(result) => Ok(result),
            Err(err) => {
                let _ = fail_candle_backfill_run(&self.pool, run_id, &err.to_string(), Utc::now())
                    .await;
                let _ = event_publisher
                    .publish(SystemEventType::MarketBackfillFailed.into_event(
                        correlation_id,
                        self.source.clone(),
                        serde_json::json!({
                            "run_id": run_id,
                            "exchange": request.exchange.as_str(),
                            "symbol": symbol.as_str(),
                            "interval": interval.as_str(),
                            "error": err.to_string(),
                        }),
                    ))
                    .await;
                Err(err)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPageRequest {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceKlineRow(
    pub i64,
    pub String,
    pub String,
    pub String,
    pub String,
    pub String,
    pub i64,
    pub String,
    pub i64,
    pub String,
    pub String,
    pub String,
);

pub fn plan_backfill_pages(
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    interval: CandleInterval,
    limit_per_request: u16,
) -> Result<Vec<BackfillPageRequest>> {
    if end_time <= start_time {
        return Err(anyhow!("backfill end_time must be after start_time"));
    }
    if limit_per_request == 0 {
        return Err(anyhow!(
            "backfill limit_per_request must be greater than zero"
        ));
    }

    let mut pages = Vec::new();
    let interval_ms = interval.duration().num_milliseconds();
    let page_span_ms = interval_ms * i64::from(limit_per_request);
    let mut cursor = start_time;
    let inclusive_end = end_time - chrono::Duration::milliseconds(1);

    while cursor < end_time {
        let page_end = std::cmp::min(
            cursor + chrono::Duration::milliseconds(page_span_ms)
                - chrono::Duration::milliseconds(1),
            inclusive_end,
        );
        pages.push(BackfillPageRequest {
            start_time: cursor,
            end_time: page_end,
        });
        cursor = page_end + chrono::Duration::milliseconds(1);
    }

    Ok(pages)
}

pub fn binance_kline_row_to_closed_candle(
    row: &BinanceKlineRow,
    exchange: MarketDataSource,
    symbol: &Symbol,
    interval: CandleInterval,
    now: DateTime<Utc>,
) -> Result<Option<Candle>> {
    let open_time = Utc
        .timestamp_millis_opt(row.0)
        .single()
        .ok_or_else(|| anyhow!("invalid Binance kline open_time"))?;
    let close_time = Utc
        .timestamp_millis_opt(row.6)
        .single()
        .ok_or_else(|| anyhow!("invalid Binance kline close_time"))?;
    let expected_close_time = open_time + interval.duration() - chrono::Duration::milliseconds(1);
    if close_time != expected_close_time || close_time >= now {
        return Ok(None);
    }

    Ok(Some(Candle {
        id: Uuid::new_v4(),
        exchange,
        symbol: symbol.clone(),
        interval,
        open_time,
        close_time,
        open: row.1.parse()?,
        high: row.2.parse()?,
        low: row.3.parse()?,
        close: row.4.parse()?,
        volume: row.5.parse()?,
        quote_volume: Some(row.7.parse()?),
        trade_count: i32::try_from(row.8).map_err(|_| anyhow!("trade_count exceeds i32"))?,
        is_closed: true,
        created_at: now,
        updated_at: now,
    }))
}

fn new_candle(
    exchange: MarketDataSource,
    symbol: Symbol,
    interval: CandleInterval,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    trade: &MarketTrade,
    quote_volume: Decimal,
) -> Candle {
    Candle {
        id: Uuid::new_v4(),
        exchange,
        symbol,
        interval,
        open_time,
        close_time,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: trade.quantity,
        quote_volume: Some(quote_volume),
        trade_count: 1,
        is_closed: false,
        created_at: trade.trade_time,
        updated_at: trade.trade_time,
    }
}

fn update_candle(candle: &mut Candle, trade: &MarketTrade, quote_volume: Decimal) {
    candle.high = candle.high.max(trade.price);
    candle.low = candle.low.min(trade.price);
    candle.close = trade.price;
    candle.volume += trade.quantity;
    candle.quote_volume = Some(candle.quote_volume.unwrap_or(Decimal::ZERO) + quote_volume);
    candle.trade_count += 1;
    candle.updated_at = trade.trade_time;
}

fn floor_time(timestamp: DateTime<Utc>, interval: CandleInterval) -> DateTime<Utc> {
    let seconds = interval.duration().num_seconds();
    let floored = timestamp.timestamp() - (timestamp.timestamp() % seconds);
    Utc.timestamp_opt(floored, 0)
        .single()
        .expect("floored candle timestamp must be valid")
}

fn chrono_duration(duration: Duration) -> Result<TimeDelta> {
    TimeDelta::from_std(duration).map_err(|err| anyhow!(err))
}

#[cfg(test)]
mod tests {
    use super::{
        binance_kline_row_to_closed_candle, parse_binance_trade_message, plan_backfill_pages,
        BinanceKlineRow, CandleBuilder, CandleBuilderError,
    };
    use aegis_core::{CandleInterval, MarketDataSource, MarketTrade, Symbol};
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;

    fn trade(price: i64, quantity: i64, second: u32) -> MarketTrade {
        MarketTrade {
            trade_id: format!("trade-{second}"),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            price: Decimal::new(price, 0),
            quantity: Decimal::new(quantity, 0),
            trade_time: Utc.with_ymd_and_hms(2026, 5, 24, 10, 0, second).unwrap(),
            received_at: Utc.with_ymd_and_hms(2026, 5, 24, 10, 0, second).unwrap(),
            is_buyer_maker: Some(false),
            raw_payload: None,
        }
    }

    #[test]
    fn first_trade_initializes_candle() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        let update = builder.apply_trade(&trade(100, 2, 1)).unwrap();

        assert_eq!(update.active.open, Decimal::new(100, 0));
        assert_eq!(update.active.high, Decimal::new(100, 0));
        assert_eq!(update.active.low, Decimal::new(100, 0));
        assert_eq!(update.active.close, Decimal::new(100, 0));
        assert_eq!(update.active.volume, Decimal::new(2, 0));
        assert_eq!(update.active.trade_count, 1);
        assert!(update.closed.is_none());
    }

    #[test]
    fn high_low_update_correctly() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        builder.apply_trade(&trade(100, 2, 1)).unwrap();
        builder.apply_trade(&trade(110, 1, 10)).unwrap();
        let update = builder.apply_trade(&trade(95, 1, 20)).unwrap();

        assert_eq!(update.active.high, Decimal::new(110, 0));
        assert_eq!(update.active.low, Decimal::new(95, 0));
    }

    #[test]
    fn close_updates_on_every_trade() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        builder.apply_trade(&trade(100, 1, 1)).unwrap();
        let update = builder.apply_trade(&trade(102, 1, 15)).unwrap();

        assert_eq!(update.active.close, Decimal::new(102, 0));
    }

    #[test]
    fn volume_accumulates() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        builder.apply_trade(&trade(100, 2, 1)).unwrap();
        let update = builder.apply_trade(&trade(101, 3, 15)).unwrap();

        assert_eq!(update.active.volume, Decimal::new(5, 0));
        assert_eq!(update.active.trade_count, 2);
    }

    #[test]
    fn interval_rollover_closes_old_candle_and_starts_new_candle() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        builder.apply_trade(&trade(100, 2, 1)).unwrap();
        let next_trade = MarketTrade {
            trade_time: Utc.with_ymd_and_hms(2026, 5, 24, 10, 1, 0).unwrap(),
            received_at: Utc.with_ymd_and_hms(2026, 5, 24, 10, 1, 0).unwrap(),
            ..trade(101, 1, 1)
        };
        let update = builder.apply_trade(&next_trade).unwrap();

        let closed = update.closed.expect("previous candle should close");
        assert!(closed.is_closed);
        assert_eq!(closed.close, Decimal::new(100, 0));
        assert_eq!(update.active.open, Decimal::new(101, 0));
        assert_eq!(update.active.trade_count, 1);
    }

    #[test]
    fn out_of_order_trade_is_rejected() {
        let mut builder = CandleBuilder::new(
            MarketDataSource::Binance,
            Symbol::new("BTCUSDT").unwrap(),
            CandleInterval::OneMinute,
        );

        let later = MarketTrade {
            trade_time: Utc.with_ymd_and_hms(2026, 5, 24, 10, 1, 0).unwrap(),
            received_at: Utc.with_ymd_and_hms(2026, 5, 24, 10, 1, 0).unwrap(),
            ..trade(101, 1, 1)
        };
        builder.apply_trade(&later).unwrap();

        let earlier = trade(99, 1, 59);
        let err = builder.apply_trade(&earlier).unwrap_err();

        assert!(matches!(err, CandleBuilderError::OutOfOrderTrade { .. }));
    }

    #[test]
    fn parses_binance_trade_message() {
        let trade = parse_binance_trade_message(
            r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1740000000000,"s":"BTCUSDT","t":123,"p":"101.25","q":"0.5","T":1740000000001,"m":true}}"#,
        )
        .unwrap();

        assert_eq!(trade.symbol.as_str(), "BTCUSDT");
        assert_eq!(trade.trade_id, "123");
        assert_eq!(trade.price, Decimal::new(10125, 2));
        assert_eq!(trade.quantity, Decimal::new(5, 1));
    }

    #[test]
    fn plans_pagination_windows_for_24_hours_of_1m_candles() {
        let start = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap();
        let pages = plan_backfill_pages(start, end, CandleInterval::OneMinute, 1000).unwrap();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].start_time, start);
        assert_eq!(pages[1].end_time, end - chrono::Duration::milliseconds(1));
    }

    #[test]
    fn parses_closed_binance_kline_into_candle() {
        let symbol = Symbol::new("BTCUSDT").unwrap();
        let row = BinanceKlineRow(
            1777593600000,
            "95000.10".to_string(),
            "95100.10".to_string(),
            "94950.00".to_string(),
            "95050.00".to_string(),
            "12.5".to_string(),
            1777593659999,
            "1188125.00".to_string(),
            321,
            "6.0".to_string(),
            "570000.00".to_string(),
            "0".to_string(),
        );

        let candle = binance_kline_row_to_closed_candle(
            &row,
            MarketDataSource::Binance,
            &symbol,
            CandleInterval::OneMinute,
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 2, 0).unwrap(),
        )
        .unwrap()
        .expect("closed candle");

        assert_eq!(candle.open, Decimal::new(9500010, 2));
        assert_eq!(candle.trade_count, 321);
        assert!(candle.is_closed);
    }

    #[test]
    fn excludes_open_or_incomplete_klines() {
        let symbol = Symbol::new("BTCUSDT").unwrap();
        let row = BinanceKlineRow(
            1777593600000,
            "95000.10".to_string(),
            "95100.10".to_string(),
            "94950.00".to_string(),
            "95050.00".to_string(),
            "12.5".to_string(),
            1777593659999,
            "1188125.00".to_string(),
            321,
            "6.0".to_string(),
            "570000.00".to_string(),
            "0".to_string(),
        );

        let candle = binance_kline_row_to_closed_candle(
            &row,
            MarketDataSource::Binance,
            &symbol,
            CandleInterval::OneMinute,
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 30).unwrap(),
        )
        .unwrap();

        assert!(candle.is_none());
    }
}
