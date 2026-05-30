use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::PgPool;

const RS_V1_STRATEGY_KIND: &str = "RELATIVE_STRENGTH_CONTINUATION_V1_RESEARCH";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesFundingRateInput {
    pub exchange: String,
    pub symbol: String,
    pub funding_time: DateTime<Utc>,
    pub funding_rate: Decimal,
    pub mark_price: Option<Decimal>,
    pub fetched_at: DateTime<Utc>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesOpenInterestInput {
    pub exchange: String,
    pub symbol: String,
    pub period: String,
    pub timestamp: DateTime<Utc>,
    pub open_interest: Decimal,
    pub open_interest_value: Option<Decimal>,
    pub fetched_at: DateTime<Utc>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesPositioningInput {
    pub exchange: String,
    pub symbol: String,
    pub metric: String,
    pub period: String,
    pub timestamp: DateTime<Utc>,
    pub long_short_ratio: Option<Decimal>,
    pub long_account: Option<Decimal>,
    pub short_account: Option<Decimal>,
    pub buy_sell_ratio: Option<Decimal>,
    pub buy_vol: Option<Decimal>,
    pub sell_vol: Option<Decimal>,
    pub fetched_at: DateTime<Utc>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivativesUpsertSummary {
    pub inserted: i32,
    pub updated: i32,
    pub skipped: i32,
}

impl DerivativesUpsertSummary {
    fn record(&mut self, outcome: UpsertOutcome) {
        match outcome {
            UpsertOutcome::Inserted => self.inserted += 1,
            UpsertOutcome::Updated => self.updated += 1,
            UpsertOutcome::Skipped => self.skipped += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpsertOutcome {
    Inserted,
    Updated,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesContextCoverage {
    pub rows: i64,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesFreshnessRow {
    pub exchange: String,
    pub symbol: String,
    pub metric: String,
    pub period: Option<String>,
    pub latest_timestamp: Option<DateTime<Utc>>,
    pub rows: i64,
    pub status: String,
    pub stale_after: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesFreshnessReport {
    pub generated_at: DateTime<Utc>,
    pub execution_authority: String,
    pub rows: Vec<DerivativesFreshnessRow>,
    pub missing_count: i32,
    pub stale_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DerivativesBackfillSummary {
    pub exchange: String,
    pub symbol: String,
    pub data_kind: String,
    pub period: Option<String>,
    pub metric: Option<String>,
    pub requested_start: Option<DateTime<Utc>>,
    pub requested_end: Option<DateTime<Utc>>,
    pub fetched_rows: i32,
    pub inserted: i32,
    pub updated: i32,
    pub skipped: i32,
    pub coverage: DerivativesContextCoverage,
    pub retention_note: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FundingBucket {
    ExtremeNegative,
    Negative,
    Neutral,
    Positive,
    ExtremePositive,
    Unknown,
}

impl FundingBucket {
    pub fn classify(rate: Option<Decimal>, zscore: Option<Decimal>) -> Self {
        if let Some(zscore) = zscore {
            if zscore <= Decimal::new(-2, 0) {
                return Self::ExtremeNegative;
            }
            if zscore >= Decimal::new(2, 0) {
                return Self::ExtremePositive;
            }
        }
        match rate {
            Some(value) if value < Decimal::ZERO => Self::Negative,
            Some(value) if value > Decimal::ZERO => Self::Positive,
            Some(_) => Self::Neutral,
            None => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PriceOiRegime {
    PriceUpOiUp,
    PriceUpOiDown,
    PriceDownOiUp,
    PriceDownOiDown,
    Unknown,
}

impl PriceOiRegime {
    pub fn classify(price_change_pct: Option<Decimal>, oi_change_pct: Option<Decimal>) -> Self {
        match (price_change_pct, oi_change_pct) {
            (Some(price), Some(oi)) if price >= Decimal::ZERO && oi >= Decimal::ZERO => {
                Self::PriceUpOiUp
            }
            (Some(price), Some(oi)) if price >= Decimal::ZERO && oi < Decimal::ZERO => {
                Self::PriceUpOiDown
            }
            (Some(price), Some(oi)) if price < Decimal::ZERO && oi >= Decimal::ZERO => {
                Self::PriceDownOiUp
            }
            (Some(_), Some(_)) => Self::PriceDownOiDown,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RsV1DerivativesTradeContext {
    pub run_id: Uuid,
    pub trade_id: Uuid,
    pub trade_index: i32,
    pub symbol: String,
    pub entry_time: DateTime<Utc>,
    pub net_pnl_pct: Decimal,
    pub winner: bool,
    pub funding_rate: Option<Decimal>,
    pub funding_zscore: Option<Decimal>,
    pub funding_bucket: FundingBucket,
    pub open_interest: Option<Decimal>,
    pub oi_24h_change_pct: Option<Decimal>,
    pub oi_72h_change_pct: Option<Decimal>,
    pub price_24h_change_pct: Option<Decimal>,
    pub price_oi_regime: PriceOiRegime,
    pub long_short_ratio: Option<Decimal>,
    pub buy_sell_ratio: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RsV1DerivativesGroupSummary {
    pub group: String,
    pub trades: i32,
    pub winners: i32,
    pub losers: i32,
    pub win_rate_pct: Option<Decimal>,
    pub avg_net_pnl_pct: Option<Decimal>,
    pub median_net_pnl_pct: Option<Decimal>,
    pub funding_mean: Option<Decimal>,
    pub funding_median: Option<Decimal>,
    pub funding_zscore_mean: Option<Decimal>,
    pub funding_zscore_median: Option<Decimal>,
    pub oi_24h_change_mean: Option<Decimal>,
    pub oi_24h_change_median: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RsV1DerivativesCoverageSummary {
    pub total_trades: i32,
    pub trades_with_funding_context: i32,
    pub trades_with_funding_zscore: i32,
    pub trades_with_oi_context: i32,
    pub trades_with_positioning_context: i32,
    pub latest_funding_timestamp: Option<DateTime<Utc>>,
    pub latest_oi_timestamp: Option<DateTime<Utc>>,
    pub latest_positioning_timestamp: Option<DateTime<Utc>>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionSafetyCounts {
    pub orders: i64,
    pub paper_positions: i64,
    pub paper_fills: i64,
    pub exchange_testnet_orders: i64,
    pub exchange_testnet_order_lifecycle_events: i64,
    pub testnet_shadow_promotions: i64,
    pub research_candidates: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RsV1DerivativesAttributionReport {
    pub generated_at: DateTime<Utc>,
    pub safety_counts_before: ExecutionSafetyCounts,
    pub safety_counts_after: ExecutionSafetyCounts,
    pub coverage: RsV1DerivativesCoverageSummary,
    pub winners: RsV1DerivativesGroupSummary,
    pub losers: RsV1DerivativesGroupSummary,
    pub by_funding_bucket: Vec<RsV1DerivativesGroupSummary>,
    pub by_price_oi_regime: Vec<RsV1DerivativesGroupSummary>,
    pub by_symbol: Vec<RsV1DerivativesGroupSummary>,
    pub by_period: Vec<RsV1DerivativesGroupSummary>,
    pub sample_trades: Vec<RsV1DerivativesTradeContext>,
    pub classification: String,
}

pub async fn upsert_derivatives_funding_rates(
    pool: &PgPool,
    rows: &[DerivativesFundingRateInput],
) -> Result<DerivativesUpsertSummary> {
    let mut summary = DerivativesUpsertSummary::default();
    for row in rows {
        summary.record(upsert_derivatives_funding_rate(pool, row).await?);
    }
    Ok(summary)
}

async fn upsert_derivatives_funding_rate(
    pool: &PgPool,
    row: &DerivativesFundingRateInput,
) -> Result<UpsertOutcome> {
    let id = Uuid::new_v4();
    let inserted = sqlx::query(
        r#"
        INSERT INTO derivatives_funding_rates (
            id, exchange, symbol, funding_time, funding_rate, mark_price, fetched_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (exchange, symbol, funding_time) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(row.funding_time)
    .bind(row.funding_rate)
    .bind(row.mark_price)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    if inserted.is_some() {
        return Ok(UpsertOutcome::Inserted);
    }

    let updated = sqlx::query(
        r#"
        UPDATE derivatives_funding_rates
        SET funding_rate = $4,
            mark_price = $5,
            fetched_at = $6,
            raw_payload = $7,
            updated_at = NOW()
        WHERE exchange = $1
          AND symbol = $2
          AND funding_time = $3
          AND (
              funding_rate IS DISTINCT FROM $4 OR
              mark_price IS DISTINCT FROM $5 OR
              raw_payload IS DISTINCT FROM $7
          )
        RETURNING id
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(row.funding_time)
    .bind(row.funding_rate)
    .bind(row.mark_price)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    Ok(if updated.is_some() {
        UpsertOutcome::Updated
    } else {
        UpsertOutcome::Skipped
    })
}

pub async fn upsert_derivatives_open_interest_snapshots(
    pool: &PgPool,
    rows: &[DerivativesOpenInterestInput],
) -> Result<DerivativesUpsertSummary> {
    let mut summary = DerivativesUpsertSummary::default();
    for row in rows {
        summary.record(upsert_derivatives_open_interest_snapshot(pool, row).await?);
    }
    Ok(summary)
}

async fn upsert_derivatives_open_interest_snapshot(
    pool: &PgPool,
    row: &DerivativesOpenInterestInput,
) -> Result<UpsertOutcome> {
    let id = Uuid::new_v4();
    let inserted = sqlx::query(
        r#"
        INSERT INTO derivatives_open_interest_snapshots (
            id, exchange, symbol, period, timestamp, open_interest,
            open_interest_value, fetched_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (exchange, symbol, period, timestamp) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(&row.period)
    .bind(row.timestamp)
    .bind(row.open_interest)
    .bind(row.open_interest_value)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    if inserted.is_some() {
        return Ok(UpsertOutcome::Inserted);
    }

    let updated = sqlx::query(
        r#"
        UPDATE derivatives_open_interest_snapshots
        SET open_interest = $5,
            open_interest_value = $6,
            fetched_at = $7,
            raw_payload = $8,
            updated_at = NOW()
        WHERE exchange = $1
          AND symbol = $2
          AND period = $3
          AND timestamp = $4
          AND (
              open_interest IS DISTINCT FROM $5 OR
              open_interest_value IS DISTINCT FROM $6 OR
              raw_payload IS DISTINCT FROM $8
          )
        RETURNING id
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(&row.period)
    .bind(row.timestamp)
    .bind(row.open_interest)
    .bind(row.open_interest_value)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    Ok(if updated.is_some() {
        UpsertOutcome::Updated
    } else {
        UpsertOutcome::Skipped
    })
}

pub async fn upsert_derivatives_positioning_snapshots(
    pool: &PgPool,
    rows: &[DerivativesPositioningInput],
) -> Result<DerivativesUpsertSummary> {
    let mut summary = DerivativesUpsertSummary::default();
    for row in rows {
        summary.record(upsert_derivatives_positioning_snapshot(pool, row).await?);
    }
    Ok(summary)
}

async fn upsert_derivatives_positioning_snapshot(
    pool: &PgPool,
    row: &DerivativesPositioningInput,
) -> Result<UpsertOutcome> {
    let id = Uuid::new_v4();
    let inserted = sqlx::query(
        r#"
        INSERT INTO derivatives_positioning_snapshots (
            id, exchange, symbol, metric, period, timestamp,
            long_short_ratio, long_account, short_account, buy_sell_ratio,
            buy_vol, sell_vol, fetched_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (exchange, symbol, metric, period, timestamp) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(id)
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(&row.metric)
    .bind(&row.period)
    .bind(row.timestamp)
    .bind(row.long_short_ratio)
    .bind(row.long_account)
    .bind(row.short_account)
    .bind(row.buy_sell_ratio)
    .bind(row.buy_vol)
    .bind(row.sell_vol)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    if inserted.is_some() {
        return Ok(UpsertOutcome::Inserted);
    }

    let updated = sqlx::query(
        r#"
        UPDATE derivatives_positioning_snapshots
        SET long_short_ratio = $6,
            long_account = $7,
            short_account = $8,
            buy_sell_ratio = $9,
            buy_vol = $10,
            sell_vol = $11,
            fetched_at = $12,
            raw_payload = $13,
            updated_at = NOW()
        WHERE exchange = $1
          AND symbol = $2
          AND metric = $3
          AND period = $4
          AND timestamp = $5
          AND (
              long_short_ratio IS DISTINCT FROM $6 OR
              long_account IS DISTINCT FROM $7 OR
              short_account IS DISTINCT FROM $8 OR
              buy_sell_ratio IS DISTINCT FROM $9 OR
              buy_vol IS DISTINCT FROM $10 OR
              sell_vol IS DISTINCT FROM $11 OR
              raw_payload IS DISTINCT FROM $13
          )
        RETURNING id
        "#,
    )
    .bind(&row.exchange)
    .bind(&row.symbol)
    .bind(&row.metric)
    .bind(&row.period)
    .bind(row.timestamp)
    .bind(row.long_short_ratio)
    .bind(row.long_account)
    .bind(row.short_account)
    .bind(row.buy_sell_ratio)
    .bind(row.buy_vol)
    .bind(row.sell_vol)
    .bind(row.fetched_at)
    .bind(&row.raw_payload)
    .fetch_optional(pool)
    .await?;
    Ok(if updated.is_some() {
        UpsertOutcome::Updated
    } else {
        UpsertOutcome::Skipped
    })
}

pub async fn derivatives_funding_coverage(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
) -> Result<DerivativesContextCoverage> {
    context_coverage(
        pool,
        "derivatives_funding_rates",
        "funding_time",
        exchange,
        symbol,
        None,
        None,
    )
    .await
}

pub async fn derivatives_open_interest_coverage(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    period: &str,
) -> Result<DerivativesContextCoverage> {
    context_coverage(
        pool,
        "derivatives_open_interest_snapshots",
        "timestamp",
        exchange,
        symbol,
        Some(("period", period)),
        None,
    )
    .await
}

pub async fn derivatives_positioning_coverage(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    metric: &str,
    period: &str,
) -> Result<DerivativesContextCoverage> {
    context_coverage(
        pool,
        "derivatives_positioning_snapshots",
        "timestamp",
        exchange,
        symbol,
        Some(("metric", metric)),
        Some(("period", period)),
    )
    .await
}

async fn context_coverage(
    pool: &PgPool,
    table: &str,
    time_column: &str,
    exchange: &str,
    symbol: &str,
    filter_a: Option<(&str, &str)>,
    filter_b: Option<(&str, &str)>,
) -> Result<DerivativesContextCoverage> {
    let mut sql = format!(
        "SELECT COUNT(*) AS rows, MIN({time_column}) AS first_timestamp, MAX({time_column}) AS last_timestamp FROM {table} WHERE exchange = $1 AND symbol = $2"
    );
    if let Some((column, _)) = filter_a {
        sql.push_str(&format!(" AND {column} = $3"));
    }
    if let Some((column, _)) = filter_b {
        sql.push_str(&format!(" AND {column} = $4"));
    }
    let mut query = sqlx::query(&sql).bind(exchange).bind(symbol);
    if let Some((_, value)) = filter_a {
        query = query.bind(value);
    }
    if let Some((_, value)) = filter_b {
        query = query.bind(value);
    }
    let row = query.fetch_one(pool).await?;
    Ok(DerivativesContextCoverage {
        rows: row.get("rows"),
        first_timestamp: row.get("first_timestamp"),
        last_timestamp: row.get("last_timestamp"),
    })
}

pub async fn execution_safety_counts(pool: &PgPool) -> Result<ExecutionSafetyCounts> {
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*) FROM orders) AS orders,
            (SELECT COUNT(*) FROM paper_positions) AS paper_positions,
            (SELECT COUNT(*) FROM paper_fills) AS paper_fills,
            (SELECT COUNT(*) FROM exchange_testnet_orders) AS exchange_testnet_orders,
            (SELECT COUNT(*) FROM exchange_testnet_order_lifecycle_events) AS exchange_testnet_order_lifecycle_events,
            (SELECT COUNT(*) FROM testnet_shadow_promotions) AS testnet_shadow_promotions,
            (SELECT COUNT(*) FROM research_candidates) AS research_candidates
        "#,
    )
    .fetch_one(pool)
    .await?;
    Ok(ExecutionSafetyCounts {
        orders: row.get("orders"),
        paper_positions: row.get("paper_positions"),
        paper_fills: row.get("paper_fills"),
        exchange_testnet_orders: row.get("exchange_testnet_orders"),
        exchange_testnet_order_lifecycle_events: row.get("exchange_testnet_order_lifecycle_events"),
        testnet_shadow_promotions: row.get("testnet_shadow_promotions"),
        research_candidates: row.get("research_candidates"),
    })
}

pub async fn derivatives_freshness_report(
    pool: &PgPool,
    symbols: &[String],
    oi_period: &str,
    positioning_period: &str,
    now: DateTime<Utc>,
) -> Result<DerivativesFreshnessReport> {
    let mut rows = Vec::new();
    let funding_stale_after = now - chrono::Duration::hours(12);
    let four_hour_stale_after = now - chrono::Duration::hours(8);
    for symbol in symbols {
        let normalized = symbol.trim().to_ascii_uppercase();
        rows.push(freshness_row(
            "binance",
            &normalized,
            "funding",
            None,
            latest_funding_timestamp(pool, "binance", &normalized).await?,
            funding_stale_after,
        ));
        rows.push(freshness_row(
            "binance",
            &normalized,
            "open_interest",
            Some(oi_period),
            latest_open_interest_timestamp(pool, "binance", &normalized, oi_period).await?,
            four_hour_stale_after,
        ));
        rows.push(freshness_row(
            "binance",
            &normalized,
            "global-long-short",
            Some(positioning_period),
            latest_positioning_timestamp(
                pool,
                "binance",
                &normalized,
                "global-long-short",
                positioning_period,
            )
            .await?,
            four_hour_stale_after,
        ));
        rows.push(freshness_row(
            "binance",
            &normalized,
            "taker-buy-sell",
            Some(positioning_period),
            latest_positioning_timestamp(
                pool,
                "binance",
                &normalized,
                "taker-buy-sell",
                positioning_period,
            )
            .await?,
            four_hour_stale_after,
        ));
    }
    let missing_count = rows.iter().filter(|row| row.status == "missing").count() as i32;
    let stale_count = rows.iter().filter(|row| row.status == "stale").count() as i32;
    Ok(DerivativesFreshnessReport {
        generated_at: now,
        execution_authority: "NONE".to_string(),
        rows,
        missing_count,
        stale_count,
    })
}

fn freshness_row(
    exchange: &str,
    symbol: &str,
    metric: &str,
    period: Option<&str>,
    latest: Option<(Option<DateTime<Utc>>, i64)>,
    stale_after: DateTime<Utc>,
) -> DerivativesFreshnessRow {
    let (latest_timestamp, rows) = latest.unwrap_or((None, 0));
    let status = match latest_timestamp {
        None => "missing",
        Some(timestamp) if timestamp < stale_after => "stale",
        Some(_) => "fresh",
    };
    DerivativesFreshnessRow {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        metric: metric.to_string(),
        period: period.map(ToOwned::to_owned),
        latest_timestamp,
        rows,
        status: status.to_string(),
        stale_after,
    }
}

async fn latest_funding_timestamp(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
) -> Result<Option<(Option<DateTime<Utc>>, i64)>> {
    latest_timestamp(
        pool,
        "derivatives_funding_rates",
        "funding_time",
        exchange,
        symbol,
        None,
        None,
    )
    .await
}

async fn latest_open_interest_timestamp(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    period: &str,
) -> Result<Option<(Option<DateTime<Utc>>, i64)>> {
    latest_timestamp(
        pool,
        "derivatives_open_interest_snapshots",
        "timestamp",
        exchange,
        symbol,
        Some(("period", period)),
        None,
    )
    .await
}

async fn latest_positioning_timestamp(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    metric: &str,
    period: &str,
) -> Result<Option<(Option<DateTime<Utc>>, i64)>> {
    latest_timestamp(
        pool,
        "derivatives_positioning_snapshots",
        "timestamp",
        exchange,
        symbol,
        Some(("metric", metric)),
        Some(("period", period)),
    )
    .await
}

async fn latest_timestamp(
    pool: &PgPool,
    table: &str,
    time_column: &str,
    exchange: &str,
    symbol: &str,
    filter_a: Option<(&str, &str)>,
    filter_b: Option<(&str, &str)>,
) -> Result<Option<(Option<DateTime<Utc>>, i64)>> {
    let mut sql = format!(
        "SELECT MAX({time_column}) AS latest_timestamp, COUNT(*) AS rows FROM {table} WHERE exchange = $1 AND symbol = $2"
    );
    if let Some((column, _)) = filter_a {
        sql.push_str(&format!(" AND {column} = $3"));
    }
    if let Some((column, _)) = filter_b {
        sql.push_str(&format!(" AND {column} = $4"));
    }
    let mut query = sqlx::query(&sql).bind(exchange).bind(symbol);
    if let Some((_, value)) = filter_a {
        query = query.bind(value);
    }
    if let Some((_, value)) = filter_b {
        query = query.bind(value);
    }
    let row = query.fetch_one(pool).await?;
    Ok(Some((row.get("latest_timestamp"), row.get("rows"))))
}

pub async fn build_rs_v1_derivatives_attribution_report(
    pool: &PgPool,
) -> Result<RsV1DerivativesAttributionReport> {
    let safety_counts_before = execution_safety_counts(pool).await?;
    let rows = sqlx::query(
        r#"
        SELECT
            t.run_id,
            t.id AS trade_id,
            t.trade_index,
            t.symbol,
            t.entry_time,
            t.net_pnl_pct,
            funding.funding_rate,
            funding_stats.funding_zscore,
            oi.open_interest,
            CASE
                WHEN oi.open_interest IS NOT NULL AND oi_24.open_interest IS NOT NULL AND oi_24.open_interest <> 0
                THEN ((oi.open_interest - oi_24.open_interest) / oi_24.open_interest) * 100
                ELSE NULL
            END AS oi_24h_change_pct,
            CASE
                WHEN oi.open_interest IS NOT NULL AND oi_72.open_interest IS NOT NULL AND oi_72.open_interest <> 0
                THEN ((oi.open_interest - oi_72.open_interest) / oi_72.open_interest) * 100
                ELSE NULL
            END AS oi_72h_change_pct,
            CASE
                WHEN price_now.close IS NOT NULL AND price_24.close IS NOT NULL AND price_24.close <> 0
                THEN ((price_now.close - price_24.close) / price_24.close) * 100
                ELSE NULL
            END AS price_24h_change_pct,
            long_short.long_short_ratio,
            taker.buy_sell_ratio
        FROM cross_asset_research_trades t
        JOIN cross_asset_research_runs r ON r.id = t.run_id
        LEFT JOIN LATERAL (
            SELECT funding_rate, funding_time
            FROM derivatives_funding_rates f
            WHERE f.exchange = 'binance'
              AND f.symbol = t.symbol
              AND f.funding_time <= t.entry_time
            ORDER BY f.funding_time DESC
            LIMIT 1
        ) funding ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                CASE
                    WHEN COUNT(*) >= 20 AND STDDEV_POP(f.funding_rate) > 0
                    THEN (funding.funding_rate - AVG(f.funding_rate)) / STDDEV_POP(f.funding_rate)
                    ELSE NULL
                END AS funding_zscore
            FROM derivatives_funding_rates f
            WHERE f.exchange = 'binance'
              AND f.symbol = t.symbol
              AND f.funding_time <= t.entry_time
              AND f.funding_time >= t.entry_time - INTERVAL '90 days'
        ) funding_stats ON TRUE
        LEFT JOIN LATERAL (
            SELECT open_interest, timestamp
            FROM derivatives_open_interest_snapshots o
            WHERE o.exchange = 'binance'
              AND o.symbol = t.symbol
              AND o.period = '4h'
              AND o.timestamp <= t.entry_time
            ORDER BY o.timestamp DESC
            LIMIT 1
        ) oi ON TRUE
        LEFT JOIN LATERAL (
            SELECT open_interest
            FROM derivatives_open_interest_snapshots o
            WHERE o.exchange = 'binance'
              AND o.symbol = t.symbol
              AND o.period = '4h'
              AND o.timestamp <= t.entry_time - INTERVAL '24 hours'
            ORDER BY o.timestamp DESC
            LIMIT 1
        ) oi_24 ON TRUE
        LEFT JOIN LATERAL (
            SELECT open_interest
            FROM derivatives_open_interest_snapshots o
            WHERE o.exchange = 'binance'
              AND o.symbol = t.symbol
              AND o.period = '4h'
              AND o.timestamp <= t.entry_time - INTERVAL '72 hours'
            ORDER BY o.timestamp DESC
            LIMIT 1
        ) oi_72 ON TRUE
        LEFT JOIN LATERAL (
            SELECT close
            FROM candles c
            WHERE c.exchange = 'binance'
              AND c.symbol = t.symbol
              AND c.interval = '4h'
              AND c.open_time <= t.entry_time
              AND c.is_closed = TRUE
            ORDER BY c.open_time DESC
            LIMIT 1
        ) price_now ON TRUE
        LEFT JOIN LATERAL (
            SELECT close
            FROM candles c
            WHERE c.exchange = 'binance'
              AND c.symbol = t.symbol
              AND c.interval = '4h'
              AND c.open_time <= t.entry_time - INTERVAL '24 hours'
              AND c.is_closed = TRUE
            ORDER BY c.open_time DESC
            LIMIT 1
        ) price_24 ON TRUE
        LEFT JOIN LATERAL (
            SELECT long_short_ratio
            FROM derivatives_positioning_snapshots p
            WHERE p.exchange = 'binance'
              AND p.symbol = t.symbol
              AND p.metric = 'global-long-short'
              AND p.period = '4h'
              AND p.timestamp <= t.entry_time
            ORDER BY p.timestamp DESC
            LIMIT 1
        ) long_short ON TRUE
        LEFT JOIN LATERAL (
            SELECT buy_sell_ratio
            FROM derivatives_positioning_snapshots p
            WHERE p.exchange = 'binance'
              AND p.symbol = t.symbol
              AND p.metric = 'taker-buy-sell'
              AND p.period = '4h'
              AND p.timestamp <= t.entry_time
            ORDER BY p.timestamp DESC
            LIMIT 1
        ) taker ON TRUE
        WHERE r.strategy_kind = $1
        ORDER BY t.entry_time ASC, t.trade_index ASC
        "#,
    )
    .bind(RS_V1_STRATEGY_KIND)
    .fetch_all(pool)
    .await?;

    let trades = rows
        .iter()
        .map(|row| {
            let net_pnl_pct: Decimal = row.get("net_pnl_pct");
            let funding_rate = row.get("funding_rate");
            let funding_zscore = row.get("funding_zscore");
            let oi_24h_change_pct = row.get("oi_24h_change_pct");
            let price_24h_change_pct = row.get("price_24h_change_pct");
            RsV1DerivativesTradeContext {
                run_id: row.get("run_id"),
                trade_id: row.get("trade_id"),
                trade_index: row.get("trade_index"),
                symbol: row.get("symbol"),
                entry_time: row.get("entry_time"),
                net_pnl_pct,
                winner: net_pnl_pct > Decimal::ZERO,
                funding_rate,
                funding_zscore,
                funding_bucket: FundingBucket::classify(funding_rate, funding_zscore),
                open_interest: row.get("open_interest"),
                oi_24h_change_pct,
                oi_72h_change_pct: row.get("oi_72h_change_pct"),
                price_24h_change_pct,
                price_oi_regime: PriceOiRegime::classify(price_24h_change_pct, oi_24h_change_pct),
                long_short_ratio: row.get("long_short_ratio"),
                buy_sell_ratio: row.get("buy_sell_ratio"),
            }
        })
        .collect::<Vec<_>>();

    let coverage = RsV1DerivativesCoverageSummary {
        total_trades: trades.len() as i32,
        trades_with_funding_context: trades
            .iter()
            .filter(|trade| trade.funding_rate.is_some())
            .count() as i32,
        trades_with_funding_zscore: trades
            .iter()
            .filter(|trade| trade.funding_zscore.is_some())
            .count() as i32,
        trades_with_oi_context: trades
            .iter()
            .filter(|trade| trade.open_interest.is_some())
            .count() as i32,
        trades_with_positioning_context: trades
            .iter()
            .filter(|trade| trade.long_short_ratio.is_some() || trade.buy_sell_ratio.is_some())
            .count() as i32,
        latest_funding_timestamp: latest_any_funding_timestamp(pool).await?,
        latest_oi_timestamp: latest_any_open_interest_timestamp(pool).await?,
        latest_positioning_timestamp: latest_any_positioning_timestamp(pool).await?,
        warnings: attribution_coverage_warnings(
            trades.len() as i32,
            trades
                .iter()
                .filter(|trade| trade.open_interest.is_some())
                .count() as i32,
            trades
                .iter()
                .filter(|trade| trade.long_short_ratio.is_some() || trade.buy_sell_ratio.is_some())
                .count() as i32,
        ),
    };

    let winners_vec = trades
        .iter()
        .filter(|trade| trade.winner)
        .cloned()
        .collect::<Vec<_>>();
    let losers_vec = trades
        .iter()
        .filter(|trade| !trade.winner)
        .cloned()
        .collect::<Vec<_>>();

    let mut by_funding_bucket =
        group_by_key(&trades, |trade| format!("{:?}", trade.funding_bucket));
    let mut by_price_oi_regime =
        group_by_key(&trades, |trade| format!("{:?}", trade.price_oi_regime));
    let mut by_symbol = group_by_key(&trades, |trade| trade.symbol.clone());
    let mut by_period = group_by_key(&trades, |trade| {
        if trade.entry_time
            < DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                .expect("static timestamp")
                .with_timezone(&Utc)
        {
            "2023-2024".to_string()
        } else {
            "2025+".to_string()
        }
    });

    by_funding_bucket.sort_by(|a, b| a.group.cmp(&b.group));
    by_price_oi_regime.sort_by(|a, b| a.group.cmp(&b.group));
    by_symbol.sort_by(|a, b| a.group.cmp(&b.group));
    by_period.sort_by(|a, b| a.group.cmp(&b.group));

    let safety_counts_after = execution_safety_counts(pool).await?;
    let classification = classify_attribution(&coverage, &winners_vec, &losers_vec);

    Ok(RsV1DerivativesAttributionReport {
        generated_at: Utc::now(),
        safety_counts_before,
        safety_counts_after,
        coverage,
        winners: summarize_group("winners", &winners_vec),
        losers: summarize_group("losers", &losers_vec),
        by_funding_bucket,
        by_price_oi_regime,
        by_symbol,
        by_period,
        sample_trades: trades.into_iter().rev().take(20).collect(),
        classification,
    })
}

async fn latest_any_open_interest_timestamp(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query(
        "SELECT MAX(timestamp) AS latest_timestamp FROM derivatives_open_interest_snapshots",
    )
    .fetch_one(pool)
    .await?
    .get("latest_timestamp"))
}

async fn latest_any_funding_timestamp(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    Ok(
        sqlx::query("SELECT MAX(funding_time) AS latest_timestamp FROM derivatives_funding_rates")
            .fetch_one(pool)
            .await?
            .get("latest_timestamp"),
    )
}

async fn latest_any_positioning_timestamp(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query(
        "SELECT MAX(timestamp) AS latest_timestamp FROM derivatives_positioning_snapshots",
    )
    .fetch_one(pool)
    .await?
    .get("latest_timestamp"))
}

fn attribution_coverage_warnings(total: i32, oi: i32, positioning: i32) -> Vec<String> {
    let mut warnings = Vec::new();
    if total > 0 && oi == 0 {
        warnings.push(
            "oi_context_missing_for_rs_v1_trades_public_retention_recent_or_forward_only"
                .to_string(),
        );
    }
    if total > 0 && positioning == 0 {
        warnings.push(
            "positioning_context_missing_for_rs_v1_trades_public_retention_recent_or_forward_only"
                .to_string(),
        );
    }
    warnings
}

fn classify_attribution(
    coverage: &RsV1DerivativesCoverageSummary,
    winners: &[RsV1DerivativesTradeContext],
    losers: &[RsV1DerivativesTradeContext],
) -> String {
    if coverage.total_trades == 0 || coverage.trades_with_funding_context == 0 {
        return "CONNECTOR_BLOCKED".to_string();
    }
    if coverage.trades_with_oi_context == 0 {
        return "FUNDING_ONLY_HISTORICAL_AVAILABLE".to_string();
    }

    let winner_funding = mean_decimal(winners.iter().filter_map(|trade| trade.funding_rate));
    let loser_funding = mean_decimal(losers.iter().filter_map(|trade| trade.funding_rate));
    let winner_oi = mean_decimal(winners.iter().filter_map(|trade| trade.oi_24h_change_pct));
    let loser_oi = mean_decimal(losers.iter().filter_map(|trade| trade.oi_24h_change_pct));
    let funding_separated = decimal_abs_diff(winner_funding, loser_funding)
        .map(|value| value >= Decimal::new(25, 6))
        .unwrap_or(false);
    let oi_separated = decimal_abs_diff(winner_oi, loser_oi)
        .map(|value| value >= Decimal::new(50, 2))
        .unwrap_or(false);

    if funding_separated || oi_separated {
        "DATA_LAYER_READY_ATTRIBUTION_PROMISING".to_string()
    } else {
        "DATA_LAYER_READY_ATTRIBUTION_WEAK".to_string()
    }
}

fn decimal_abs_diff(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    Some((left? - right?).abs())
}

fn group_by_key<F>(
    trades: &[RsV1DerivativesTradeContext],
    mut key_fn: F,
) -> Vec<RsV1DerivativesGroupSummary>
where
    F: FnMut(&RsV1DerivativesTradeContext) -> String,
{
    let mut groups = BTreeMap::<String, Vec<RsV1DerivativesTradeContext>>::new();
    for trade in trades {
        groups.entry(key_fn(trade)).or_default().push(trade.clone());
    }
    groups
        .into_iter()
        .map(|(group, trades)| summarize_group(&group, &trades))
        .collect()
}

fn summarize_group(
    group: &str,
    trades: &[RsV1DerivativesTradeContext],
) -> RsV1DerivativesGroupSummary {
    let winners = trades.iter().filter(|trade| trade.winner).count() as i32;
    let losers = trades.len() as i32 - winners;
    RsV1DerivativesGroupSummary {
        group: group.to_string(),
        trades: trades.len() as i32,
        winners,
        losers,
        win_rate_pct: if trades.is_empty() {
            None
        } else {
            Some(Decimal::from(winners) * Decimal::new(100, 0) / Decimal::from(trades.len() as i32))
        },
        avg_net_pnl_pct: mean_decimal(trades.iter().map(|trade| trade.net_pnl_pct)),
        median_net_pnl_pct: median_decimal(trades.iter().map(|trade| trade.net_pnl_pct)),
        funding_mean: mean_decimal(trades.iter().filter_map(|trade| trade.funding_rate)),
        funding_median: median_decimal(trades.iter().filter_map(|trade| trade.funding_rate)),
        funding_zscore_mean: mean_decimal(trades.iter().filter_map(|trade| trade.funding_zscore)),
        funding_zscore_median: median_decimal(
            trades.iter().filter_map(|trade| trade.funding_zscore),
        ),
        oi_24h_change_mean: mean_decimal(trades.iter().filter_map(|trade| trade.oi_24h_change_pct)),
        oi_24h_change_median: median_decimal(
            trades.iter().filter_map(|trade| trade.oi_24h_change_pct),
        ),
    }
}

fn mean_decimal<I>(values: I) -> Option<Decimal>
where
    I: Iterator<Item = Decimal>,
{
    let mut count = Decimal::ZERO;
    let mut sum = Decimal::ZERO;
    for value in values {
        count += Decimal::ONE;
        sum += value;
    }
    if count == Decimal::ZERO {
        None
    } else {
        Some(sum / count)
    }
}

fn median_decimal<I>(values: I) -> Option<Decimal>
where
    I: Iterator<Item = Decimal>,
{
    let mut values = values.collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort();
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[middle - 1] + values[middle]) / Decimal::new(2, 0))
    } else {
        Some(values[middle])
    }
}

#[cfg(test)]
mod tests {
    use super::{FundingBucket, PriceOiRegime};
    use rust_decimal::Decimal;

    #[test]
    fn funding_bucket_prefers_extreme_zscore() {
        assert_eq!(
            FundingBucket::classify(Some(Decimal::ZERO), Some(Decimal::new(21, 1))),
            FundingBucket::ExtremePositive
        );
        assert_eq!(
            FundingBucket::classify(Some(Decimal::ZERO), Some(Decimal::new(-21, 1))),
            FundingBucket::ExtremeNegative
        );
    }

    #[test]
    fn funding_bucket_classifies_rate_sign() {
        assert_eq!(
            FundingBucket::classify(Some(Decimal::new(-1, 4)), None),
            FundingBucket::Negative
        );
        assert_eq!(
            FundingBucket::classify(Some(Decimal::new(1, 4)), None),
            FundingBucket::Positive
        );
    }

    #[test]
    fn price_oi_regime_classifies_quadrants() {
        assert_eq!(
            PriceOiRegime::classify(Some(Decimal::ONE), Some(Decimal::ONE)),
            PriceOiRegime::PriceUpOiUp
        );
        assert_eq!(
            PriceOiRegime::classify(Some(Decimal::ONE), Some(Decimal::NEGATIVE_ONE)),
            PriceOiRegime::PriceUpOiDown
        );
        assert_eq!(
            PriceOiRegime::classify(Some(Decimal::NEGATIVE_ONE), Some(Decimal::ONE)),
            PriceOiRegime::PriceDownOiUp
        );
        assert_eq!(
            PriceOiRegime::classify(Some(Decimal::NEGATIVE_ONE), Some(Decimal::NEGATIVE_ONE)),
            PriceOiRegime::PriceDownOiDown
        );
    }

    #[test]
    fn freshness_row_detects_missing_and_stale() {
        let now = chrono::Utc::now();
        let stale_after = now - chrono::Duration::hours(1);
        let missing =
            super::freshness_row("binance", "BTCUSDT", "funding", None, None, stale_after);
        assert_eq!(missing.status, "missing");
        let stale = super::freshness_row(
            "binance",
            "BTCUSDT",
            "funding",
            None,
            Some((Some(stale_after - chrono::Duration::minutes(1)), 1)),
            stale_after,
        );
        assert_eq!(stale.status, "stale");
        let fresh = super::freshness_row(
            "binance",
            "BTCUSDT",
            "funding",
            None,
            Some((Some(now), 1)),
            stale_after,
        );
        assert_eq!(fresh.status, "fresh");
    }
}
