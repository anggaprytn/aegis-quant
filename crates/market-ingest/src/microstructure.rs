use std::{collections::BTreeMap, str::FromStr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, TimeZone, Timelike, Utc};
use db::{
    MicrostructureImbalanceMetricInput, MicrostructureLiquidityMetricInput,
    MicrostructurePersistBatch, MicrostructureSpreadMetricInput, MicrostructureUpsertSummary,
};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_USDM_WS_BASE_URL: &str = "wss://fstream.binance.com";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrostructureCollectRequest {
    pub exchange: String,
    pub market_type: String,
    pub symbols: Vec<String>,
    pub bucket_seconds: i32,
    pub duration_seconds: u64,
    pub dry_run: bool,
    pub ws_base_url: String,
    pub include_force_order: bool,
}

impl MicrostructureCollectRequest {
    pub fn new(
        exchange: impl Into<String>,
        market_type: impl Into<String>,
        symbols: Vec<String>,
        bucket_seconds: i32,
        duration_seconds: u64,
        dry_run: bool,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            market_type: market_type.into(),
            symbols,
            bucket_seconds,
            duration_seconds,
            dry_run,
            ws_base_url: DEFAULT_USDM_WS_BASE_URL.to_string(),
            include_force_order: true,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().to_ascii_lowercase() != "binance" {
            bail!("microstructure collector only supports --exchange binance");
        }
        if self.market_type.trim().to_ascii_lowercase() != "usdm" {
            bail!("microstructure collector only supports --market-type usdm");
        }
        if self.symbols.is_empty() {
            bail!("at least one symbol is required");
        }
        if self.bucket_seconds <= 0 {
            bail!("--bucket-seconds must be positive");
        }
        if self.duration_seconds == 0 {
            bail!("--duration-seconds must be positive");
        }
        Ok(())
    }

    pub fn normalized_symbols(&self) -> Vec<String> {
        self.symbols
            .iter()
            .map(|symbol| symbol.trim().to_ascii_uppercase())
            .filter(|symbol| !symbol.is_empty())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrostructureCollectResult {
    pub exchange: String,
    pub market_type: String,
    pub symbols: Vec<String>,
    pub bucket_seconds: i32,
    pub duration_seconds: u64,
    pub dry_run: bool,
    pub started_at: DateTime<Utc>,
    pub stopped_at: DateTime<Utc>,
    pub observed_events: i64,
    pub book_ticker_events: i64,
    pub depth_events: i64,
    pub aggregate_trade_events: i64,
    pub mark_price_events: i64,
    pub liquidation_events: i64,
    pub spread_rows: i64,
    pub imbalance_rows: i64,
    pub liquidity_rows: i64,
    pub persist_summary: Option<MicrostructureUpsertSummary>,
}

#[derive(Debug, Clone)]
pub struct BinanceUsdMMicrostructureCollector {
    request: MicrostructureCollectRequest,
}

impl BinanceUsdMMicrostructureCollector {
    pub fn new(request: MicrostructureCollectRequest) -> Self {
        Self { request }
    }

    pub fn public_stream_url(&self) -> String {
        let streams = self
            .request
            .normalized_symbols()
            .into_iter()
            .flat_map(|symbol| {
                let lower = symbol.to_ascii_lowercase();
                [
                    format!("{lower}@bookTicker"),
                    format!("{lower}@depth20@500ms"),
                ]
            })
            .collect::<Vec<_>>()
            .join("/");

        format!(
            "{}/public/stream?streams={streams}",
            self.request.ws_base_url.trim_end_matches('/')
        )
    }

    pub fn market_stream_url(&self) -> String {
        let streams = self
            .request
            .normalized_symbols()
            .into_iter()
            .flat_map(|symbol| {
                let lower = symbol.to_ascii_lowercase();
                let mut streams =
                    vec![format!("{lower}@aggTrade"), format!("{lower}@markPrice@1s")];
                if self.request.include_force_order {
                    streams.push(format!("{lower}@forceOrder"));
                }
                streams
            })
            .collect::<Vec<_>>()
            .join("/");

        format!(
            "{}/market/stream?streams={streams}",
            self.request.ws_base_url.trim_end_matches('/')
        )
    }

    pub async fn collect(
        &self,
    ) -> Result<(MicrostructureCollectResult, MicrostructurePersistBatch)> {
        self.request.validate()?;
        let symbols = self.request.normalized_symbols();
        let started_at = Utc::now();
        let mut aggregator = MicrostructureAggregator::new(
            self.request.exchange.trim().to_ascii_lowercase(),
            self.request.market_type.trim().to_ascii_lowercase(),
            symbols.clone(),
            self.request.bucket_seconds,
        );

        let (tx, mut rx) = mpsc::channel::<Result<StreamMessage>>(4096);
        let public_url = self.public_stream_url();
        let market_url = self.market_stream_url();
        tokio::spawn(read_stream(public_url, tx.clone()));
        tokio::spawn(read_stream(market_url, tx));

        let deadline = tokio::time::sleep(Duration::from_secs(self.request.duration_seconds));
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => break,
                maybe_message = rx.recv() => {
                    let Some(message) = maybe_message else {
                        bail!("microstructure websocket readers stopped before duration completed");
                    };
                    aggregator.apply(message?)?;
                }
            }
        }

        let stopped_at = Utc::now();
        let batch = aggregator.build_persist_batch();
        let result = MicrostructureCollectResult {
            exchange: self.request.exchange.trim().to_ascii_lowercase(),
            market_type: self.request.market_type.trim().to_ascii_lowercase(),
            symbols,
            bucket_seconds: self.request.bucket_seconds,
            duration_seconds: self.request.duration_seconds,
            dry_run: self.request.dry_run,
            started_at,
            stopped_at,
            observed_events: aggregator.event_counts.observed_events,
            book_ticker_events: aggregator.event_counts.book_ticker_events,
            depth_events: aggregator.event_counts.depth_events,
            aggregate_trade_events: aggregator.event_counts.aggregate_trade_events,
            mark_price_events: aggregator.event_counts.mark_price_events,
            liquidation_events: aggregator.event_counts.liquidation_events,
            spread_rows: batch.spread.len() as i64,
            imbalance_rows: batch.imbalance.len() as i64,
            liquidity_rows: batch.liquidity.len() as i64,
            persist_summary: None,
        };
        Ok((result, batch))
    }
}

#[derive(Debug)]
struct StreamMessage {
    stream: String,
    data: Value,
}

async fn read_stream(url: String, tx: mpsc::Sender<Result<StreamMessage>>) {
    let result: Result<()> = async {
        let (mut socket, _) = match connect_async(&url).await {
            Ok(connected) => connected,
            Err(primary_err) if url.starts_with("wss://fstream.binance.com/") => {
                let fallback_url = url.replacen(
                    "wss://fstream.binance.com/",
                    "wss://fstream.binancefuture.com/",
                    1,
                );
                connect_async(&fallback_url).await.with_context(|| {
                    format!("connect {url}; fallback {fallback_url}; primary error: {primary_err}")
                })?
            }
            Err(err) => return Err(err).with_context(|| format!("connect {url}")),
        };
        while let Some(message) = socket.next().await {
            match message.with_context(|| format!("read {url}"))? {
                Message::Text(text) => {
                    let envelope = parse_stream_message(&text)?;
                    if tx.send(Ok(envelope)).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => bail!("websocket closed: {url}"),
                Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            }
        }
        Ok(())
    }
    .await;

    if let Err(err) = result {
        let _ = tx.send(Err(err)).await;
    }
}

fn parse_stream_message(text: &str) -> Result<StreamMessage> {
    let value: Value = serde_json::from_str(text)?;
    let stream = value
        .get("stream")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("combined stream message missing stream"))?
        .to_string();
    let data = value
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow!("combined stream message missing data"))?;
    Ok(StreamMessage { stream, data })
}

#[derive(Debug, Clone, Default)]
struct EventCounts {
    observed_events: i64,
    book_ticker_events: i64,
    depth_events: i64,
    aggregate_trade_events: i64,
    mark_price_events: i64,
    liquidation_events: i64,
}

#[derive(Debug, Clone)]
pub struct MicrostructureAggregator {
    exchange: String,
    market_type: String,
    symbols: Vec<String>,
    bucket_seconds: i32,
    symbol_state: BTreeMap<String, SymbolState>,
    buckets: BTreeMap<(String, DateTime<Utc>), BucketAccumulator>,
    event_counts: EventCounts,
}

impl MicrostructureAggregator {
    pub fn new(
        exchange: String,
        market_type: String,
        symbols: Vec<String>,
        bucket_seconds: i32,
    ) -> Self {
        Self {
            exchange,
            market_type,
            symbols,
            bucket_seconds,
            symbol_state: BTreeMap::new(),
            buckets: BTreeMap::new(),
            event_counts: EventCounts::default(),
        }
    }

    fn apply(&mut self, message: StreamMessage) -> Result<()> {
        self.event_counts.observed_events += 1;
        if message.stream.contains("@bookTicker") {
            self.event_counts.book_ticker_events += 1;
            return self.apply_book_ticker(&message.data);
        }
        if message.stream.contains("@depth20") {
            self.event_counts.depth_events += 1;
            return self.apply_depth(&message.data);
        }
        if message.stream.contains("@aggTrade") {
            self.event_counts.aggregate_trade_events += 1;
            return self.apply_aggregate_trade(&message.data);
        }
        if message.stream.contains("@markPrice") {
            self.event_counts.mark_price_events += 1;
            return Ok(());
        }
        if message.stream.contains("@forceOrder") {
            self.event_counts.liquidation_events += 1;
            return self.apply_force_order(&message.data);
        }
        Ok(())
    }

    pub fn apply_book_ticker_value(&mut self, data: &Value) -> Result<()> {
        self.event_counts.observed_events += 1;
        self.event_counts.book_ticker_events += 1;
        self.apply_book_ticker(data)
    }

    pub fn apply_depth_value(&mut self, data: &Value) -> Result<()> {
        self.event_counts.observed_events += 1;
        self.event_counts.depth_events += 1;
        self.apply_depth(data)
    }

    pub fn apply_aggregate_trade_value(&mut self, data: &Value) -> Result<()> {
        self.event_counts.observed_events += 1;
        self.event_counts.aggregate_trade_events += 1;
        self.apply_aggregate_trade(data)
    }

    fn apply_book_ticker(&mut self, data: &Value) -> Result<()> {
        let symbol = required_string(data, "s")?.to_ascii_uppercase();
        let event_time = event_time(data);
        let best_bid_price = required_decimal(data, "b")?;
        let best_bid_qty = required_decimal(data, "B")?;
        let best_ask_price = required_decimal(data, "a")?;
        let best_ask_qty = required_decimal(data, "A")?;

        let state = self.symbol_state.entry(symbol.clone()).or_default();
        state.best_bid_price = Some(best_bid_price);
        state.best_bid_qty = Some(best_bid_qty);
        state.best_ask_price = Some(best_ask_price);
        state.best_ask_qty = Some(best_ask_qty);

        let bucket_seconds = self.bucket_seconds;
        let bucket = self.bucket_mut(&symbol, event_time);
        bucket.record_spread(
            bucket_seconds,
            best_bid_price,
            best_bid_qty,
            best_ask_price,
            best_ask_qty,
        );
        Ok(())
    }

    fn apply_depth(&mut self, data: &Value) -> Result<()> {
        let symbol = required_string(data, "s")?.to_ascii_uppercase();
        let event_time = event_time(data);
        let bids = parse_depth_levels(data, "b")?;
        let asks = parse_depth_levels(data, "a")?;
        if bids.is_empty() || asks.is_empty() {
            return Ok(());
        }

        let best_bid_price = bids[0].price;
        let best_bid_qty = bids[0].qty;
        let best_ask_price = asks[0].price;
        let best_ask_qty = asks[0].qty;
        let state = self.symbol_state.entry(symbol.clone()).or_default();
        state.best_bid_price = Some(best_bid_price);
        state.best_bid_qty = Some(best_bid_qty);
        state.best_ask_price = Some(best_ask_price);
        state.best_ask_qty = Some(best_ask_qty);
        state.bids = bids.clone();
        state.asks = asks.clone();

        let bucket_seconds = self.bucket_seconds;
        let bucket = self.bucket_mut(&symbol, event_time);
        bucket.record_spread(
            bucket_seconds,
            best_bid_price,
            best_bid_qty,
            best_ask_price,
            best_ask_qty,
        );
        bucket.record_depth(bids, asks);
        Ok(())
    }

    fn apply_aggregate_trade(&mut self, data: &Value) -> Result<()> {
        let symbol = required_string(data, "s")?.to_ascii_uppercase();
        let event_time = event_time(data);
        let price = required_decimal(data, "p")?;
        let quantity = required_decimal(data, "q")?;
        let notional = price * quantity;
        let buyer_is_maker = data.get("m").and_then(Value::as_bool).unwrap_or(false);
        let state = self.symbol_state.get(&symbol).cloned().unwrap_or_default();
        let bucket = self.bucket_mut(&symbol, event_time);
        if buyer_is_maker {
            bucket.aggressive_sell_notional += notional;
            bucket.aggressive_sell_count += 1;
            if state
                .best_bid_price
                .zip(state.best_bid_qty)
                .map(|(best_bid_price, best_bid_qty)| {
                    price <= best_bid_price && quantity >= best_bid_qty
                })
                .unwrap_or(false)
            {
                bucket.sweep_sell_count += 1;
            }
        } else {
            bucket.aggressive_buy_notional += notional;
            bucket.aggressive_buy_count += 1;
            if state
                .best_ask_price
                .zip(state.best_ask_qty)
                .map(|(best_ask_price, best_ask_qty)| {
                    price >= best_ask_price && quantity >= best_ask_qty
                })
                .unwrap_or(false)
            {
                bucket.sweep_buy_count += 1;
            }
        }
        Ok(())
    }

    fn apply_force_order(&mut self, data: &Value) -> Result<()> {
        let order = data
            .get("o")
            .ok_or_else(|| anyhow!("forceOrder payload missing order object"))?;
        let symbol = required_string(order, "s")?.to_ascii_uppercase();
        let event_time = event_time(data);
        let side = required_string(order, "S")?.to_ascii_uppercase();
        let price = optional_decimal(order, "ap")
            .or_else(|| optional_decimal(order, "p"))
            .unwrap_or(Decimal::ZERO);
        let quantity = optional_decimal(order, "z")
            .or_else(|| optional_decimal(order, "q"))
            .unwrap_or(Decimal::ZERO);
        let notional = price * quantity;
        let bucket = self.bucket_mut(&symbol, event_time);
        match side.as_str() {
            "BUY" => {
                bucket.liquidation_buy_count += 1;
                bucket.liquidation_buy_notional += notional;
            }
            "SELL" => {
                bucket.liquidation_sell_count += 1;
                bucket.liquidation_sell_notional += notional;
            }
            _ => {}
        }
        Ok(())
    }

    fn bucket_mut(&mut self, symbol: &str, event_time: DateTime<Utc>) -> &mut BucketAccumulator {
        let bucket_start = floor_to_bucket(event_time, self.bucket_seconds);
        self.buckets
            .entry((symbol.to_string(), bucket_start))
            .or_insert_with(|| BucketAccumulator::new(bucket_start))
    }

    pub fn build_persist_batch(&self) -> MicrostructurePersistBatch {
        let mut spread = Vec::new();
        let mut imbalance = Vec::new();
        let mut liquidity = Vec::new();

        for symbol in &self.symbols {
            for ((bucket_symbol, _), bucket) in self
                .buckets
                .iter()
                .filter(|((bucket_symbol, _), _)| bucket_symbol == symbol)
            {
                if let Some(row) = bucket.spread_metric(
                    &self.exchange,
                    &self.market_type,
                    bucket_symbol,
                    self.bucket_seconds,
                ) {
                    spread.push(row);
                }
                if let Some(row) = bucket.imbalance_metric(
                    &self.exchange,
                    &self.market_type,
                    bucket_symbol,
                    self.bucket_seconds,
                ) {
                    imbalance.push(row);
                }
                liquidity.push(bucket.liquidity_metric(
                    &self.exchange,
                    &self.market_type,
                    bucket_symbol,
                    self.bucket_seconds,
                ));
            }
        }

        MicrostructurePersistBatch {
            spread,
            imbalance,
            liquidity,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SymbolState {
    best_bid_price: Option<Decimal>,
    best_bid_qty: Option<Decimal>,
    best_ask_price: Option<Decimal>,
    best_ask_qty: Option<Decimal>,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone, Copy, Default)]
struct DepthLevel {
    price: Decimal,
    qty: Decimal,
}

#[derive(Debug, Clone)]
struct BucketAccumulator {
    bucket_start: DateTime<Utc>,
    best_bid_price: Option<Decimal>,
    best_ask_price: Option<Decimal>,
    mid_price: Option<Decimal>,
    spread_abs: Option<Decimal>,
    spread_bps: Option<Decimal>,
    spread_bps_sum: Decimal,
    spread_high_bps: Option<Decimal>,
    spread_low_bps: Option<Decimal>,
    update_count: i32,
    locked_count: i32,
    crossed_count: i32,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
    aggressive_buy_notional: Decimal,
    aggressive_sell_notional: Decimal,
    aggressive_buy_count: i32,
    aggressive_sell_count: i32,
    sweep_buy_count: i32,
    sweep_sell_count: i32,
    liquidation_buy_count: i32,
    liquidation_sell_count: i32,
    liquidation_buy_notional: Decimal,
    liquidation_sell_notional: Decimal,
}

impl BucketAccumulator {
    fn new(bucket_start: DateTime<Utc>) -> Self {
        Self {
            bucket_start,
            best_bid_price: None,
            best_ask_price: None,
            mid_price: None,
            spread_abs: None,
            spread_bps: None,
            spread_bps_sum: Decimal::ZERO,
            spread_high_bps: None,
            spread_low_bps: None,
            update_count: 0,
            locked_count: 0,
            crossed_count: 0,
            bids: Vec::new(),
            asks: Vec::new(),
            aggressive_buy_notional: Decimal::ZERO,
            aggressive_sell_notional: Decimal::ZERO,
            aggressive_buy_count: 0,
            aggressive_sell_count: 0,
            sweep_buy_count: 0,
            sweep_sell_count: 0,
            liquidation_buy_count: 0,
            liquidation_sell_count: 0,
            liquidation_buy_notional: Decimal::ZERO,
            liquidation_sell_notional: Decimal::ZERO,
        }
    }

    fn record_spread(
        &mut self,
        bucket_seconds: i32,
        best_bid_price: Decimal,
        _best_bid_qty: Decimal,
        best_ask_price: Decimal,
        _best_ask_qty: Decimal,
    ) {
        let two = Decimal::from(2);
        let ten_thousand = Decimal::from(10_000);
        let mid_price = (best_bid_price + best_ask_price) / two;
        let spread_abs = best_ask_price - best_bid_price;
        let spread_bps = if mid_price > Decimal::ZERO {
            spread_abs / mid_price * ten_thousand
        } else {
            Decimal::ZERO
        };

        self.best_bid_price = Some(best_bid_price);
        self.best_ask_price = Some(best_ask_price);
        self.mid_price = Some(mid_price);
        self.spread_abs = Some(spread_abs);
        self.spread_bps = Some(spread_bps);
        self.spread_bps_sum += spread_bps;
        self.spread_high_bps = Some(max_decimal(self.spread_high_bps, spread_bps));
        self.spread_low_bps = Some(min_decimal(self.spread_low_bps, spread_bps));
        self.update_count += 1;
        if spread_abs == Decimal::ZERO {
            self.locked_count += 1;
        }
        if spread_abs < Decimal::ZERO {
            self.crossed_count += 1;
        }

        let _ = bucket_seconds;
    }

    fn record_depth(&mut self, bids: Vec<DepthLevel>, asks: Vec<DepthLevel>) {
        self.bids = bids;
        self.asks = asks;
    }

    fn spread_metric(
        &self,
        exchange: &str,
        market_type: &str,
        symbol: &str,
        bucket_seconds: i32,
    ) -> Option<MicrostructureSpreadMetricInput> {
        let update_count = self.update_count;
        if update_count <= 0 {
            return None;
        }
        Some(MicrostructureSpreadMetricInput {
            exchange: exchange.to_string(),
            market_type: market_type.to_string(),
            symbol: symbol.to_string(),
            bucket_start: self.bucket_start,
            bucket_seconds,
            best_bid_price: self.best_bid_price?,
            best_ask_price: self.best_ask_price?,
            mid_price: self.mid_price?,
            spread_abs: self.spread_abs?,
            spread_bps: self.spread_bps?,
            spread_avg_bps: self.spread_bps_sum / Decimal::from(update_count),
            spread_high_bps: self.spread_high_bps?,
            spread_low_bps: self.spread_low_bps?,
            update_count,
            locked_count: self.locked_count,
            crossed_count: self.crossed_count,
        })
    }

    fn imbalance_metric(
        &self,
        exchange: &str,
        market_type: &str,
        symbol: &str,
        bucket_seconds: i32,
    ) -> Option<MicrostructureImbalanceMetricInput> {
        if self.bids.is_empty() || self.asks.is_empty() {
            return None;
        }
        let bid_qty = sum_qty(&self.bids);
        let ask_qty = sum_qty(&self.asks);
        let bid_notional = sum_notional(&self.bids);
        let ask_notional = sum_notional(&self.asks);
        let qty_imbalance = ratio_difference(bid_qty, ask_qty);
        let notional_imbalance = ratio_difference(bid_notional, ask_notional);

        Some(MicrostructureImbalanceMetricInput {
            exchange: exchange.to_string(),
            market_type: market_type.to_string(),
            symbol: symbol.to_string(),
            bucket_start: self.bucket_start,
            bucket_seconds,
            depth_levels: self.bids.len().min(self.asks.len()) as i32,
            bid_qty,
            ask_qty,
            bid_notional,
            ask_notional,
            qty_imbalance,
            notional_imbalance,
            depth_skew_bps: notional_imbalance * Decimal::from(10_000),
        })
    }

    fn liquidity_metric(
        &self,
        exchange: &str,
        market_type: &str,
        symbol: &str,
        bucket_seconds: i32,
    ) -> MicrostructureLiquidityMetricInput {
        let mid_price =
            self.mid_price
                .unwrap_or_else(|| match (self.best_bid_price, self.best_ask_price) {
                    (Some(bid), Some(ask)) => (bid + ask) / Decimal::from(2),
                    _ => Decimal::ZERO,
                });

        let bid_notional_10bps = side_notional_within_bps(&self.bids, mid_price, 10, true);
        let ask_notional_10bps = side_notional_within_bps(&self.asks, mid_price, 10, false);
        let bid_notional_25bps = side_notional_within_bps(&self.bids, mid_price, 25, true);
        let ask_notional_25bps = side_notional_within_bps(&self.asks, mid_price, 25, false);
        let bid_notional_50bps = side_notional_within_bps(&self.bids, mid_price, 50, true);
        let ask_notional_50bps = side_notional_within_bps(&self.asks, mid_price, 50, false);
        let inner = bid_notional_25bps + ask_notional_25bps;
        let outer = bid_notional_50bps + ask_notional_50bps;
        let liquidity_vacuum_score = if outer <= Decimal::ZERO {
            Decimal::from(10_000)
        } else {
            let score = (Decimal::from(1) - (inner / outer)) * Decimal::from(10_000);
            if score < Decimal::ZERO {
                Decimal::ZERO
            } else {
                score
            }
        };

        MicrostructureLiquidityMetricInput {
            exchange: exchange.to_string(),
            market_type: market_type.to_string(),
            symbol: symbol.to_string(),
            bucket_start: self.bucket_start,
            bucket_seconds,
            bid_notional_10bps,
            ask_notional_10bps,
            bid_notional_25bps,
            ask_notional_25bps,
            bid_notional_50bps,
            ask_notional_50bps,
            liquidity_vacuum_score,
            aggressive_buy_notional: self.aggressive_buy_notional,
            aggressive_sell_notional: self.aggressive_sell_notional,
            aggressive_buy_count: self.aggressive_buy_count,
            aggressive_sell_count: self.aggressive_sell_count,
            sweep_buy_count: self.sweep_buy_count,
            sweep_sell_count: self.sweep_sell_count,
            liquidation_buy_count: self.liquidation_buy_count,
            liquidation_sell_count: self.liquidation_sell_count,
            liquidation_buy_notional: self.liquidation_buy_notional,
            liquidation_sell_notional: self.liquidation_sell_notional,
        }
    }
}

fn parse_depth_levels(data: &Value, field: &str) -> Result<Vec<DepthLevel>> {
    let levels = data
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("depth payload missing {field}"))?;
    levels
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| anyhow!("depth level is not an array"))?;
            let price = values
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("depth level missing price"))
                .and_then(parse_decimal)?;
            let qty = values
                .get(1)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("depth level missing qty"))
                .and_then(parse_decimal)?;
            Ok(DepthLevel { price, qty })
        })
        .collect()
}

fn required_string<'a>(data: &'a Value, field: &str) -> Result<&'a str> {
    data.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("payload missing string field {field}"))
}

fn required_decimal(data: &Value, field: &str) -> Result<Decimal> {
    let raw = required_string(data, field)?;
    parse_decimal(raw)
}

fn optional_decimal(data: &Value, field: &str) -> Option<Decimal> {
    data.get(field)
        .and_then(Value::as_str)
        .and_then(|value| parse_decimal(value).ok())
}

fn parse_decimal(value: &str) -> Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid decimal {value:?}"))
}

fn event_time(data: &Value) -> DateTime<Utc> {
    data.get("E")
        .or_else(|| data.get("T"))
        .and_then(Value::as_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
        .unwrap_or_else(Utc::now)
}

fn floor_to_bucket(timestamp: DateTime<Utc>, bucket_seconds: i32) -> DateTime<Utc> {
    let bucket_seconds = i64::from(bucket_seconds);
    let seconds = timestamp.timestamp();
    let bucket = seconds - seconds.rem_euclid(bucket_seconds);
    Utc.timestamp_opt(bucket, 0)
        .single()
        .unwrap_or_else(|| timestamp.with_nanosecond(0).unwrap_or(timestamp))
}

fn max_decimal(current: Option<Decimal>, next: Decimal) -> Decimal {
    current.map(|value| value.max(next)).unwrap_or(next)
}

fn min_decimal(current: Option<Decimal>, next: Decimal) -> Decimal {
    current.map(|value| value.min(next)).unwrap_or(next)
}

fn sum_qty(levels: &[DepthLevel]) -> Decimal {
    levels.iter().map(|level| level.qty).sum()
}

fn sum_notional(levels: &[DepthLevel]) -> Decimal {
    levels.iter().map(|level| level.price * level.qty).sum()
}

fn ratio_difference(left: Decimal, right: Decimal) -> Decimal {
    let total = left + right;
    if total <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (left - right) / total
    }
}

fn side_notional_within_bps(
    levels: &[DepthLevel],
    mid_price: Decimal,
    bps: i32,
    is_bid: bool,
) -> Decimal {
    if mid_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let threshold = Decimal::from(bps) / Decimal::from(10_000);
    levels
        .iter()
        .filter(|level| {
            if is_bid {
                level.price <= mid_price && (mid_price - level.price) / mid_price <= threshold
            } else {
                level.price >= mid_price && (level.price - mid_price) / mid_price <= threshold
            }
        })
        .map(|level| level.price * level.qty)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn aggregates_spread_depth_and_taker_flow_without_float_math() {
        let mut aggregator = MicrostructureAggregator::new(
            "binance".to_string(),
            "usdm".to_string(),
            vec!["BTCUSDT".to_string()],
            5,
        );

        aggregator
            .apply_book_ticker_value(&json!({
                "E": 1_700_000_000_123i64,
                "s": "BTCUSDT",
                "b": "100.00",
                "B": "2.00",
                "a": "100.10",
                "A": "3.00"
            }))
            .unwrap();
        aggregator
            .apply_depth_value(&json!({
                "E": 1_700_000_000_456i64,
                "s": "BTCUSDT",
                "b": [["100.00", "2.00"], ["99.95", "1.00"]],
                "a": [["100.10", "3.00"], ["100.15", "1.00"]]
            }))
            .unwrap();
        aggregator
            .apply_aggregate_trade_value(&json!({
                "E": 1_700_000_000_789i64,
                "s": "BTCUSDT",
                "p": "100.10",
                "q": "3.00",
                "m": false
            }))
            .unwrap();

        let batch = aggregator.build_persist_batch();
        assert_eq!(batch.spread.len(), 1);
        assert_eq!(batch.imbalance.len(), 1);
        assert_eq!(batch.liquidity.len(), 1);
        assert_eq!(batch.liquidity[0].aggressive_buy_count, 1);
        assert_eq!(batch.liquidity[0].sweep_buy_count, 1);
        assert!(batch.spread[0].spread_avg_bps > Decimal::ZERO);
    }
}
