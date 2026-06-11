use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::PgPool;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureCollectorRunInput {
    pub id: Uuid,
    pub exchange: String,
    pub market_type: String,
    pub symbols: Vec<String>,
    pub started_at: DateTime<Utc>,
    pub status: String,
    pub config_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureCollectorRunRecord {
    pub id: Uuid,
    pub exchange: String,
    pub market_type: String,
    pub symbols: Vec<String>,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub status: String,
    pub config_json: Value,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureSpreadMetricInput {
    pub exchange: String,
    pub market_type: String,
    pub symbol: String,
    pub bucket_start: DateTime<Utc>,
    pub bucket_seconds: i32,
    pub best_bid_price: Decimal,
    pub best_ask_price: Decimal,
    pub mid_price: Decimal,
    pub spread_abs: Decimal,
    pub spread_bps: Decimal,
    pub spread_avg_bps: Decimal,
    pub spread_high_bps: Decimal,
    pub spread_low_bps: Decimal,
    pub update_count: i32,
    pub locked_count: i32,
    pub crossed_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureImbalanceMetricInput {
    pub exchange: String,
    pub market_type: String,
    pub symbol: String,
    pub bucket_start: DateTime<Utc>,
    pub bucket_seconds: i32,
    pub depth_levels: i32,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
    pub bid_notional: Decimal,
    pub ask_notional: Decimal,
    pub qty_imbalance: Decimal,
    pub notional_imbalance: Decimal,
    pub depth_skew_bps: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureLiquidityMetricInput {
    pub exchange: String,
    pub market_type: String,
    pub symbol: String,
    pub bucket_start: DateTime<Utc>,
    pub bucket_seconds: i32,
    pub bid_notional_10bps: Decimal,
    pub ask_notional_10bps: Decimal,
    pub bid_notional_25bps: Decimal,
    pub ask_notional_25bps: Decimal,
    pub bid_notional_50bps: Decimal,
    pub ask_notional_50bps: Decimal,
    pub liquidity_vacuum_score: Decimal,
    pub aggressive_buy_notional: Decimal,
    pub aggressive_sell_notional: Decimal,
    pub aggressive_buy_count: i32,
    pub aggressive_sell_count: i32,
    pub sweep_buy_count: i32,
    pub sweep_sell_count: i32,
    pub liquidation_buy_count: i32,
    pub liquidation_sell_count: i32,
    pub liquidation_buy_notional: Decimal,
    pub liquidation_sell_notional: Decimal,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MicrostructureUpsertSummary {
    pub spread_rows: i64,
    pub imbalance_rows: i64,
    pub liquidity_rows: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructurePersistBatch {
    pub spread: Vec<MicrostructureSpreadMetricInput>,
    pub imbalance: Vec<MicrostructureImbalanceMetricInput>,
    pub liquidity: Vec<MicrostructureLiquidityMetricInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureSummaryRow {
    pub symbol: String,
    pub spread_rows: i64,
    pub imbalance_rows: i64,
    pub liquidity_rows: i64,
    pub first_bucket: Option<DateTime<Utc>>,
    pub last_bucket: Option<DateTime<Utc>>,
    pub avg_spread_bps: Option<Decimal>,
    pub avg_notional_imbalance: Option<Decimal>,
    pub max_liquidity_vacuum: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicrostructureSummaryReport {
    pub exchange: String,
    pub market_type: String,
    pub bucket_seconds: i32,
    pub generated_at: DateTime<Utc>,
    pub rows: Vec<MicrostructureSummaryRow>,
    pub latest_collector_run: Option<MicrostructureCollectorRunRecord>,
}

pub async fn insert_microstructure_collector_run(
    pool: &PgPool,
    input: &MicrostructureCollectorRunInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO microstructure_collector_runs (
            id, exchange, market_type, symbols, started_at, status, config_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(input.id)
    .bind(&input.exchange)
    .bind(&input.market_type)
    .bind(&input.symbols)
    .bind(input.started_at)
    .bind(&input.status)
    .bind(&input.config_json)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn finish_microstructure_collector_run(
    pool: &PgPool,
    run_id: Uuid,
    status: &str,
    stopped_at: DateTime<Utc>,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE microstructure_collector_runs
        SET stopped_at = $2,
            status = $3,
            error = $4
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(stopped_at)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_microstructure_metrics(
    pool: &PgPool,
    batch: &MicrostructurePersistBatch,
) -> Result<MicrostructureUpsertSummary> {
    let mut summary = MicrostructureUpsertSummary::default();
    for row in &batch.spread {
        upsert_microstructure_spread_metric(pool, row).await?;
        summary.spread_rows += 1;
    }
    for row in &batch.imbalance {
        upsert_microstructure_imbalance_metric(pool, row).await?;
        summary.imbalance_rows += 1;
    }
    for row in &batch.liquidity {
        upsert_microstructure_liquidity_metric(pool, row).await?;
        summary.liquidity_rows += 1;
    }
    Ok(summary)
}

async fn upsert_microstructure_spread_metric(
    pool: &PgPool,
    row: &MicrostructureSpreadMetricInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO microstructure_spread_metrics (
            exchange, market_type, symbol, bucket_start, bucket_seconds,
            best_bid_price, best_ask_price, mid_price, spread_abs, spread_bps,
            spread_avg_bps, spread_high_bps, spread_low_bps,
            update_count, locked_count, crossed_count
        )
        VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13,
            $14, $15, $16
        )
        ON CONFLICT (exchange, market_type, symbol, bucket_start, bucket_seconds)
        DO UPDATE SET
            best_bid_price = EXCLUDED.best_bid_price,
            best_ask_price = EXCLUDED.best_ask_price,
            mid_price = EXCLUDED.mid_price,
            spread_abs = EXCLUDED.spread_abs,
            spread_bps = EXCLUDED.spread_bps,
            spread_avg_bps = EXCLUDED.spread_avg_bps,
            spread_high_bps = EXCLUDED.spread_high_bps,
            spread_low_bps = EXCLUDED.spread_low_bps,
            update_count = EXCLUDED.update_count,
            locked_count = EXCLUDED.locked_count,
            crossed_count = EXCLUDED.crossed_count
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.market_type)
    .bind(&row.symbol)
    .bind(row.bucket_start)
    .bind(row.bucket_seconds)
    .bind(row.best_bid_price)
    .bind(row.best_ask_price)
    .bind(row.mid_price)
    .bind(row.spread_abs)
    .bind(row.spread_bps)
    .bind(row.spread_avg_bps)
    .bind(row.spread_high_bps)
    .bind(row.spread_low_bps)
    .bind(row.update_count)
    .bind(row.locked_count)
    .bind(row.crossed_count)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_microstructure_imbalance_metric(
    pool: &PgPool,
    row: &MicrostructureImbalanceMetricInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO microstructure_imbalance_metrics (
            exchange, market_type, symbol, bucket_start, bucket_seconds,
            depth_levels, bid_qty, ask_qty, bid_notional, ask_notional,
            qty_imbalance, notional_imbalance, depth_skew_bps
        )
        VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13
        )
        ON CONFLICT (exchange, market_type, symbol, bucket_start, bucket_seconds)
        DO UPDATE SET
            depth_levels = EXCLUDED.depth_levels,
            bid_qty = EXCLUDED.bid_qty,
            ask_qty = EXCLUDED.ask_qty,
            bid_notional = EXCLUDED.bid_notional,
            ask_notional = EXCLUDED.ask_notional,
            qty_imbalance = EXCLUDED.qty_imbalance,
            notional_imbalance = EXCLUDED.notional_imbalance,
            depth_skew_bps = EXCLUDED.depth_skew_bps
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.market_type)
    .bind(&row.symbol)
    .bind(row.bucket_start)
    .bind(row.bucket_seconds)
    .bind(row.depth_levels)
    .bind(row.bid_qty)
    .bind(row.ask_qty)
    .bind(row.bid_notional)
    .bind(row.ask_notional)
    .bind(row.qty_imbalance)
    .bind(row.notional_imbalance)
    .bind(row.depth_skew_bps)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_microstructure_liquidity_metric(
    pool: &PgPool,
    row: &MicrostructureLiquidityMetricInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO microstructure_liquidity_metrics (
            exchange, market_type, symbol, bucket_start, bucket_seconds,
            bid_notional_10bps, ask_notional_10bps,
            bid_notional_25bps, ask_notional_25bps,
            bid_notional_50bps, ask_notional_50bps,
            liquidity_vacuum_score,
            aggressive_buy_notional, aggressive_sell_notional,
            aggressive_buy_count, aggressive_sell_count,
            sweep_buy_count, sweep_sell_count,
            liquidation_buy_count, liquidation_sell_count,
            liquidation_buy_notional, liquidation_sell_notional
        )
        VALUES (
            $1, $2, $3, $4, $5,
            $6, $7,
            $8, $9,
            $10, $11,
            $12,
            $13, $14,
            $15, $16,
            $17, $18,
            $19, $20,
            $21, $22
        )
        ON CONFLICT (exchange, market_type, symbol, bucket_start, bucket_seconds)
        DO UPDATE SET
            bid_notional_10bps = EXCLUDED.bid_notional_10bps,
            ask_notional_10bps = EXCLUDED.ask_notional_10bps,
            bid_notional_25bps = EXCLUDED.bid_notional_25bps,
            ask_notional_25bps = EXCLUDED.ask_notional_25bps,
            bid_notional_50bps = EXCLUDED.bid_notional_50bps,
            ask_notional_50bps = EXCLUDED.ask_notional_50bps,
            liquidity_vacuum_score = EXCLUDED.liquidity_vacuum_score,
            aggressive_buy_notional = EXCLUDED.aggressive_buy_notional,
            aggressive_sell_notional = EXCLUDED.aggressive_sell_notional,
            aggressive_buy_count = EXCLUDED.aggressive_buy_count,
            aggressive_sell_count = EXCLUDED.aggressive_sell_count,
            sweep_buy_count = EXCLUDED.sweep_buy_count,
            sweep_sell_count = EXCLUDED.sweep_sell_count,
            liquidation_buy_count = EXCLUDED.liquidation_buy_count,
            liquidation_sell_count = EXCLUDED.liquidation_sell_count,
            liquidation_buy_notional = EXCLUDED.liquidation_buy_notional,
            liquidation_sell_notional = EXCLUDED.liquidation_sell_notional
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.market_type)
    .bind(&row.symbol)
    .bind(row.bucket_start)
    .bind(row.bucket_seconds)
    .bind(row.bid_notional_10bps)
    .bind(row.ask_notional_10bps)
    .bind(row.bid_notional_25bps)
    .bind(row.ask_notional_25bps)
    .bind(row.bid_notional_50bps)
    .bind(row.ask_notional_50bps)
    .bind(row.liquidity_vacuum_score)
    .bind(row.aggressive_buy_notional)
    .bind(row.aggressive_sell_notional)
    .bind(row.aggressive_buy_count)
    .bind(row.aggressive_sell_count)
    .bind(row.sweep_buy_count)
    .bind(row.sweep_sell_count)
    .bind(row.liquidation_buy_count)
    .bind(row.liquidation_sell_count)
    .bind(row.liquidation_buy_notional)
    .bind(row.liquidation_sell_notional)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn microstructure_summary_report(
    pool: &PgPool,
    exchange: &str,
    market_type: &str,
    bucket_seconds: i32,
    symbols: &[String],
) -> Result<MicrostructureSummaryReport> {
    let mut rows = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let row = sqlx::query(
            r#"
            SELECT
                $4::TEXT AS symbol,
                (SELECT COUNT(*) FROM microstructure_spread_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS spread_rows,
                (SELECT COUNT(*) FROM microstructure_imbalance_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS imbalance_rows,
                (SELECT COUNT(*) FROM microstructure_liquidity_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS liquidity_rows,
                (SELECT MIN(bucket_start) FROM microstructure_spread_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS first_bucket,
                (SELECT MAX(bucket_start) FROM microstructure_spread_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS last_bucket,
                (SELECT AVG(spread_avg_bps) FROM microstructure_spread_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS avg_spread_bps,
                (SELECT AVG(notional_imbalance) FROM microstructure_imbalance_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS avg_notional_imbalance,
                (SELECT MAX(liquidity_vacuum_score) FROM microstructure_liquidity_metrics
                    WHERE exchange = $1 AND market_type = $2 AND bucket_seconds = $3 AND symbol = $4) AS max_liquidity_vacuum
            "#,
        )
        .bind(exchange)
        .bind(market_type)
        .bind(bucket_seconds)
        .bind(symbol)
        .fetch_one(pool)
        .await?;

        rows.push(MicrostructureSummaryRow {
            symbol: row.get("symbol"),
            spread_rows: row.get("spread_rows"),
            imbalance_rows: row.get("imbalance_rows"),
            liquidity_rows: row.get("liquidity_rows"),
            first_bucket: row.get("first_bucket"),
            last_bucket: row.get("last_bucket"),
            avg_spread_bps: row.get("avg_spread_bps"),
            avg_notional_imbalance: row.get("avg_notional_imbalance"),
            max_liquidity_vacuum: row.get("max_liquidity_vacuum"),
        });
    }

    let latest_collector_run =
        latest_microstructure_collector_run(pool, exchange, market_type).await?;

    Ok(MicrostructureSummaryReport {
        exchange: exchange.to_string(),
        market_type: market_type.to_string(),
        bucket_seconds,
        generated_at: Utc::now(),
        rows,
        latest_collector_run,
    })
}

pub async fn latest_microstructure_collector_run(
    pool: &PgPool,
    exchange: &str,
    market_type: &str,
) -> Result<Option<MicrostructureCollectorRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT id, exchange, market_type, symbols, started_at, stopped_at,
               status, config_json, error, created_at
        FROM microstructure_collector_runs
        WHERE exchange = $1 AND market_type = $2
        ORDER BY started_at DESC
        LIMIT 1
        "#,
    )
    .bind(exchange)
    .bind(market_type)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| MicrostructureCollectorRunRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        market_type: row.get("market_type"),
        symbols: row.get("symbols"),
        started_at: row.get("started_at"),
        stopped_at: row.get("stopped_at"),
        status: row.get("status"),
        config_json: row.get("config_json"),
        error: row.get("error"),
        created_at: row.get("created_at"),
    }))
}
