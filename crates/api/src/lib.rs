use std::{env, net::SocketAddr};

use accounting::{
    apply_paper_order_fill, create_default_paper_account_if_missing, PaperAccountingConfig,
};
use aegis_core::{
    CandleInterval, MarketMode, PaperAccount, PaperFill, PositionSide, RiskRejectionReason,
    StrategyConfig, StrategyId, Symbol,
};
use anyhow::Result;
use chrono::Utc;
use db::{
    get_default_paper_account, get_open_paper_position, get_strategy_status, insert_paper_account,
    insert_paper_equity_snapshot, insert_paper_fill, insert_paper_trade_journal_entry,
    paper_account_from_record, paper_position_from_record, strategy_config_from_record,
    upsert_paper_position, upsert_strategy_config, OrderRecord, PgPool, StateActor,
};
use market_ingest::MarketIngestConfig;
use rust_decimal::Decimal;
use strategy_engine::build_default_strategy_configs;

pub mod pipeline;

pub const DEFAULT_PAPER_ACCOUNT_NAME: &str = "Default Paper";
pub const DEFAULT_PAPER_ACCOUNT_BASE_CURRENCY: &str = "USDT";
pub const DEFAULT_PAPER_ACCOUNT_INITIAL_EQUITY: i64 = 1_000_000;

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub db_pool: PgPool,
    pub started_at: chrono::DateTime<Utc>,
    pub market_mode: MarketMode,
    pub market_config: MarketIngestConfig,
    pub strategy_runtime: StrategyRuntimeConfig,
}

#[derive(Clone)]
pub struct AppConfig {
    pub app_name: String,
    pub environment: String,
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub database_max_connections: u32,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, String> {
        let app_name = env::var("APP_NAME").unwrap_or_else(|_| "aegis-quant-api".to_string());
        let environment = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let bind_addr = env::var("API_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
            .parse()
            .map_err(|err| format!("invalid API_BIND_ADDR: {err}"))?;
        let database_url =
            env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_string())?;
        let database_max_connections = env::var("DATABASE_MAX_CONNECTIONS")
            .ok()
            .map(|value| {
                value
                    .parse()
                    .map_err(|err| format!("invalid DATABASE_MAX_CONNECTIONS: {err}"))
            })
            .transpose()?
            .unwrap_or(5);

        Ok(Self {
            app_name,
            environment,
            bind_addr,
            database_url,
            database_max_connections,
        })
    }
}

#[derive(Clone)]
pub struct StrategyRuntimeConfig {
    pub default_symbols: Vec<Symbol>,
    pub default_timeframe: CandleInterval,
    pub default_notional: Decimal,
    pub momentum_lookback_candles: u32,
    pub breakout_lookback_candles: u32,
}

impl StrategyRuntimeConfig {
    pub fn from_env() -> Result<Self, String> {
        let default_symbols = env::var("STRATEGY_DEFAULT_SYMBOLS")
            .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
            .split(',')
            .map(Symbol::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())?;
        let default_timeframe = env::var("STRATEGY_DEFAULT_TIMEFRAME")
            .unwrap_or_else(|_| "1m".to_string())
            .parse()
            .map_err(|err: aegis_core::CoreError| err.to_string())?;
        let default_notional = env::var("STRATEGY_DEFAULT_NOTIONAL")
            .unwrap_or_else(|_| "100000".to_string())
            .parse::<Decimal>()
            .map_err(|err| format!("invalid STRATEGY_DEFAULT_NOTIONAL: {err}"))?;
        let momentum_lookback_candles = env::var("MOMENTUM_LOOKBACK_CANDLES")
            .unwrap_or_else(|_| "3".to_string())
            .parse::<u32>()
            .map_err(|err| format!("invalid MOMENTUM_LOOKBACK_CANDLES: {err}"))?;
        let breakout_lookback_candles = env::var("BREAKOUT_LOOKBACK_CANDLES")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<u32>()
            .map_err(|err| format!("invalid BREAKOUT_LOOKBACK_CANDLES: {err}"))?;

        Ok(Self {
            default_symbols,
            default_timeframe,
            default_notional,
            momentum_lookback_candles,
            breakout_lookback_candles,
        })
    }

    pub fn default_configs(&self) -> Vec<StrategyConfig> {
        build_default_strategy_configs(
            self.default_symbols.clone(),
            self.default_timeframe,
            self.default_notional,
            self.momentum_lookback_candles,
            self.breakout_lookback_candles,
        )
    }
}

pub async fn ensure_strategy_configs(state: &AppState) -> Result<Vec<StrategyConfig>> {
    let mut configs = Vec::new();
    for config in state.strategy_runtime.default_configs() {
        let record = upsert_strategy_config(&state.db_pool, &config).await?;
        configs.push(strategy_config_from_record(&record)?);
    }

    Ok(configs)
}

pub async fn ensure_strategy_config(
    state: &AppState,
    strategy_id: StrategyId,
) -> Result<StrategyConfig> {
    let _ = ensure_strategy_configs(state).await?;
    let record = get_strategy_status(&state.db_pool, strategy_id)
        .await?
        .map(|status| status.config)
        .ok_or_else(|| anyhow::anyhow!("strategy config not found after initialization"))?;
    Ok(strategy_config_from_record(&record)?)
}

pub fn default_actor() -> StateActor {
    StateActor::system("anonymous")
}

pub fn reason_code(reason: RiskRejectionReason) -> &'static str {
    match reason {
        RiskRejectionReason::KillSwitchActive => "kill_switch_active",
        RiskRejectionReason::MaxOpenPositionsExceeded => "max_open_positions_exceeded",
        RiskRejectionReason::MaxDailyLossExceeded => "max_daily_loss_exceeded",
        RiskRejectionReason::SignalTooOld => "signal_too_old",
        RiskRejectionReason::DuplicateOrderDetected => "duplicate_order_detected",
        RiskRejectionReason::DataStale => "data_stale",
        RiskRejectionReason::PositionNotionalExceeded => "position_notional_exceeded",
        RiskRejectionReason::UnsupportedState => "unsupported_state",
    }
}

pub async fn ensure_default_paper_account(pool: &PgPool) -> Result<PaperAccount> {
    let existing = get_default_paper_account(pool)
        .await?
        .map(|record| paper_account_from_record(&record))
        .transpose()?;
    let now = Utc::now();
    let account = create_default_paper_account_if_missing(
        existing,
        DEFAULT_PAPER_ACCOUNT_NAME,
        DEFAULT_PAPER_ACCOUNT_BASE_CURRENCY,
        rust_decimal::Decimal::new(DEFAULT_PAPER_ACCOUNT_INITIAL_EQUITY, 0),
        now,
    )?;
    let record = insert_paper_account(pool, &account).await?;
    paper_account_from_record(&record)
}

pub async fn persist_paper_fill_accounting(
    pool: &PgPool,
    order: &OrderRecord,
) -> Result<Option<PaperAccount>> {
    if order.execution_state != "PAPER_FILLED" {
        return Ok(None);
    }

    let account = ensure_default_paper_account(pool).await?;
    let existing_position =
        get_open_paper_position(pool, account.id, &order.symbol, PositionSide::Long)
            .await?
            .map(|record| paper_position_from_record(&record))
            .transpose()?;
    let price = order
        .filled_price
        .or(order.avg_fill_price)
        .or(order.limit_price)
        .unwrap_or(order.quantity);
    let fill = PaperFill {
        id: uuid::Uuid::new_v4(),
        account_id: account.id,
        order_id: order.order_id,
        position_id: existing_position.as_ref().map(|position| position.id),
        symbol: order.symbol.clone(),
        side: PositionSide::Long,
        price,
        quantity: order.filled_qty,
        notional: price * order.filled_qty,
        fee: rust_decimal::Decimal::ZERO,
        slippage_cost: rust_decimal::Decimal::ZERO,
        filled_at: order.filled_at.unwrap_or_else(Utc::now),
        strategy_id: order.strategy_id.clone(),
        signal_id: order.signal_id,
        risk_decision_id: Some(order.risk_decision_id),
        correlation_id: order.correlation_id,
    };

    let application = apply_paper_order_fill(
        &account,
        existing_position.as_ref(),
        &fill,
        PaperAccountingConfig::default(),
    )?;

    let position_record = upsert_paper_position(pool, &application.position).await?;
    let fill = PaperFill {
        position_id: Some(position_record.id),
        ..application.fill
    };
    insert_paper_fill(pool, &fill).await?;
    let account_record = insert_paper_account(pool, &application.account).await?;
    for entry in application.journal_entries {
        insert_paper_trade_journal_entry(pool, &entry).await?;
    }
    insert_paper_equity_snapshot(
        pool,
        &aegis_core::PaperEquitySnapshot {
            id: uuid::Uuid::new_v4(),
            account_id: account.id,
            equity: application.summary.equity,
            realized_pnl: application.summary.realized_pnl,
            unrealized_pnl: application.summary.unrealized_pnl,
            drawdown_pct: application.summary.drawdown_pct,
            snapshot_at: fill.filled_at,
        },
    )
    .await?;

    Ok(Some(paper_account_from_record(&account_record)?))
}
