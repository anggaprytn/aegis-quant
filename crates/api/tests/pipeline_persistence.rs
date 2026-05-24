use std::{net::SocketAddr, time::Duration};

use aegis_core::{
    Candle, CandleInterval, DataFreshnessStatus, FeedStatus, MarketDataSource, MarketMode,
    PaperTradingPipelineRequest, PipelineDecision, PipelineStepStatus, StrategyConfig, StrategyId,
    StrategyMode, StrategyStatus, Symbol,
};
use api::{pipeline::run_paper_pipeline, AppConfig, AppState, StrategyRuntimeConfig};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use db::{
    get_default_paper_account, get_order_by_id, get_risk_decision, list_open_paper_positions,
    list_orders, list_paper_equity_snapshots, list_paper_trade_journal, list_recent_signals,
    list_recent_system_events, set_kill_switch_state, test_support::TestDatabase, upsert_candle,
    upsert_market_feed_status, upsert_strategy_config, StateActor,
};
use market_ingest::MarketIngestConfig;
use rust_decimal::Decimal;
use uuid::Uuid;

fn runtime_config() -> StrategyRuntimeConfig {
    StrategyRuntimeConfig {
        default_symbols: vec![Symbol::new("BTCUSDT").expect("valid symbol")],
        default_timeframe: CandleInterval::OneMinute,
        default_notional: Decimal::new(100_000, 0),
        momentum_lookback_candles: 3,
        breakout_lookback_candles: 20,
    }
}

fn strategy_config() -> StrategyConfig {
    StrategyConfig {
        strategy_id: StrategyId::MomentumV1,
        status: StrategyStatus::Enabled,
        mode: StrategyMode::SignalOnly,
        symbols: vec![Symbol::new("BTCUSDT").expect("valid symbol")],
        timeframe: CandleInterval::OneMinute,
        suggested_notional: Decimal::new(100_000, 0),
        momentum_lookback_candles: 3,
        breakout_lookback_candles: 20,
        stop_loss_pct: None,
        take_profit_pct: None,
    }
}

fn app_state(pool: db::PgPool) -> AppState {
    AppState {
        config: AppConfig {
            app_name: "aegis-quant-api".to_string(),
            environment: "test".to_string(),
            bind_addr: "127.0.0.1:3000"
                .parse::<SocketAddr>()
                .expect("valid bind addr"),
            database_url: "postgres://unused".to_string(),
            database_max_connections: 5,
        },
        db_pool: pool,
        started_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        market_mode: MarketMode::Paper,
        market_config: MarketIngestConfig {
            exchange: MarketDataSource::Binance,
            symbols: vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            stale_threshold: Duration::from_secs(10),
            binance_ws_base_url: "wss://example.invalid".to_string(),
            binance_rest_base_url: "https://example.invalid".to_string(),
        },
        strategy_runtime: runtime_config(),
    }
}

async fn seed_pipeline_happy_path(pool: &db::PgPool) {
    let symbol = Symbol::new("BTCUSDT").expect("valid symbol");
    upsert_strategy_config(pool, &strategy_config())
        .await
        .expect("strategy config should persist");
    upsert_market_feed_status(
        pool,
        MarketDataSource::Binance,
        &symbol,
        FeedStatus::Connected,
        DataFreshnessStatus::Fresh,
        Some(Utc::now()),
        None,
        0,
    )
    .await
    .expect("feed status should persist");

    let base_open = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let closes = [100_000_i64, 101_000, 102_000, 103_000];
    for (index, close) in closes.into_iter().enumerate() {
        let open_time = base_open + ChronoDuration::minutes(index as i64);
        let candle = Candle {
            id: Uuid::from_u128(0x900 + index as u128),
            exchange: MarketDataSource::Binance,
            symbol: symbol.clone(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + ChronoDuration::minutes(1),
            open: Decimal::new(close - 500, 0),
            high: Decimal::new(close + 200, 0),
            low: Decimal::new(close - 700, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(close * 10, 0)),
            trade_count: 5,
            is_closed: true,
            created_at: open_time + ChronoDuration::seconds(59),
            updated_at: open_time + ChronoDuration::seconds(59),
        };
        upsert_candle(pool, &candle)
            .await
            .expect("candle should persist");
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn pipeline_persists_signal_risk_order_and_trace() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    seed_pipeline_happy_path(&test_db.pool).await;
    let state = app_state(test_db.pool.clone());
    let correlation_id = Uuid::from_u128(0xa01);

    let result = run_paper_pipeline(
        &state,
        PaperTradingPipelineRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            correlation_id: Some(correlation_id),
        },
    )
    .await
    .expect("pipeline should succeed");

    assert_eq!(
        result.pipeline_decision,
        PipelineDecision::PaperOrderCreated
    );
    assert!(result.signal_generated);
    assert_eq!(
        result.trace.strategy_evaluation,
        PipelineStepStatus::Completed
    );
    assert_eq!(result.trace.signal, PipelineStepStatus::Completed);
    assert_eq!(result.trace.risk_evaluation, PipelineStepStatus::Completed);
    assert_eq!(result.trace.paper_order, PipelineStepStatus::Completed);

    let signal_id = result.signal_id.expect("signal id should exist");
    let risk_decision_id = result
        .risk_decision_id
        .expect("risk decision id should exist");
    let paper_order_id = result.paper_order_id.expect("paper order id should exist");

    let symbol = Symbol::new("BTCUSDT").expect("valid symbol");
    let signals = list_recent_signals(&state.db_pool, Some(&symbol), 10)
        .await
        .expect("signals should list");
    assert!(signals.iter().any(|signal| signal.id == signal_id));

    let risk = get_risk_decision(&state.db_pool, risk_decision_id)
        .await
        .expect("risk query should succeed")
        .expect("risk decision should exist");
    assert_eq!(risk.decision, "APPROVED");
    assert_eq!(risk.signal_id, Some(signal_id));

    let order = get_order_by_id(&state.db_pool, paper_order_id)
        .await
        .expect("order query should succeed")
        .expect("order should exist");
    assert_eq!(order.risk_decision_id, risk_decision_id);
    assert!(!order.idempotency_key.is_empty());

    let events = list_recent_system_events(&state.db_pool, 50)
        .await
        .expect("events should list");
    let event_types = events
        .iter()
        .filter(|event| event.correlation_id == correlation_id)
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"paper_pipeline.started"));
    assert!(event_types.contains(&"signal.generated"));
    assert!(event_types.contains(&"risk.approved"));
    assert!(event_types.contains(&"order.paper_filled"));
    assert!(event_types.contains(&"paper.fill.created"));
    assert!(event_types.contains(&"paper.position.opened"));
    assert!(event_types.contains(&"paper.equity.updated"));
    assert!(event_types.contains(&"paper_pipeline.paper_order_created"));

    let account = get_default_paper_account(&state.db_pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    let positions = list_open_paper_positions(&state.db_pool, account.id)
        .await
        .expect("paper positions should list");
    assert_eq!(positions.len(), 1);
    let equity = list_paper_equity_snapshots(&state.db_pool, account.id, 10)
        .await
        .expect("paper equity snapshots should list");
    assert!(!equity.is_empty());
    let journal = list_paper_trade_journal(&state.db_pool, account.id, 20)
        .await
        .expect("paper journal should list");
    assert!(!journal.is_empty());
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn kill_switch_rejects_pipeline_without_creating_order() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    seed_pipeline_happy_path(&test_db.pool).await;
    set_kill_switch_state(
        &test_db.pool,
        &StateActor::system("integration-test"),
        Uuid::from_u128(0xb01),
        "pipeline_persistence",
        true,
        Some("maintenance".to_string()),
    )
    .await
    .expect("kill switch should persist");
    let state = app_state(test_db.pool.clone());
    let correlation_id = Uuid::from_u128(0xb02);

    let result = run_paper_pipeline(
        &state,
        PaperTradingPipelineRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            correlation_id: Some(correlation_id),
        },
    )
    .await
    .expect("pipeline should return risk rejection");

    assert_eq!(result.pipeline_decision, PipelineDecision::RiskRejected);
    assert!(result.signal_id.is_some());
    assert!(result.risk_decision_id.is_some());
    assert!(result.paper_order_id.is_none());
    assert!(result
        .reasons
        .iter()
        .any(|reason| reason == "kill_switch_active"));
    assert_eq!(result.trace.risk_evaluation, PipelineStepStatus::Rejected);
    assert_eq!(result.trace.paper_order, PipelineStepStatus::Skipped);

    let risk = get_risk_decision(
        &state.db_pool,
        result.risk_decision_id.expect("risk decision should exist"),
    )
    .await
    .expect("risk query should succeed")
    .expect("risk decision should exist");
    assert_eq!(risk.decision, "REJECTED");

    let orders = list_orders(&state.db_pool)
        .await
        .expect("orders should list");
    assert!(orders.is_empty());
    let account = get_default_paper_account(&state.db_pool)
        .await
        .expect("paper account query should succeed");
    assert!(account.is_none());

    let events = list_recent_system_events(&state.db_pool, 50)
        .await
        .expect("events should list");
    let event_types = events
        .iter()
        .filter(|event| event.correlation_id == correlation_id)
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"paper_pipeline.started"));
    assert!(event_types.contains(&"signal.generated"));
    assert!(event_types.contains(&"risk.rejected"));
    assert!(event_types.contains(&"paper_pipeline.risk_rejected"));
}
