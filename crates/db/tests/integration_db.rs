use aegis_core::{
    BacktestEquityPoint, BacktestRequest, BacktestResult, BacktestTrade, Candle, CandleInterval,
    EventEnvelope, MarketDataSource, OrderIntent, ReplayRunStatus, RiskCheckContext,
    RiskEvaluationDecision, RiskEvaluationResult, RiskRuleDecision, RiskRuleResult, Side,
    SignalConfidence, SignalReason, SignalSide, StrategyId, StrategySignal, Symbol,
};
use chrono::{TimeZone, Utc};
use db::{
    create_paper_order, get_backtest_equity_curve, get_backtest_run, get_backtest_trades,
    get_closed_candles_range, get_order_by_idempotency_key, get_risk_decision, get_system_state,
    insert_backtest_equity_points, insert_backtest_run, insert_backtest_trade,
    insert_risk_decision, insert_signal_deduped, list_orders, list_recent_signals,
    set_kill_switch_state, test_support::TestDatabase, update_backtest_run_completed,
    upsert_candle, CreateOrderError, StateActor,
};
use rust_decimal::Decimal;
use serde_json::Value;
use uuid::Uuid;

fn fixed_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 3, 0).unwrap()
}

fn sample_signal() -> StrategySignal {
    StrategySignal {
        signal_id: Uuid::from_u128(0x101),
        strategy_id: StrategyId::MomentumV1,
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
        side: SignalSide::Buy,
        confidence: SignalConfidence::new(Decimal::new(65, 2)).expect("valid confidence"),
        timeframe: CandleInterval::OneMinute,
        reason: SignalReason::ThreeConsecutiveHigherCloses,
        suggested_notional: Decimal::new(100_000, 0),
        stop_loss_pct: None,
        take_profit_pct: None,
        source_candle_open_time: fixed_time(),
        correlation_id: Uuid::from_u128(0x201),
        created_at: fixed_time(),
    }
}

fn sample_risk_context(signal_id: Uuid, correlation_id: Uuid) -> RiskCheckContext {
    RiskCheckContext {
        signal_id,
        correlation_id,
        strategy_id: "momentum_v1".to_string(),
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
        side: Side::Buy,
        suggested_notional: Decimal::new(100_000, 0),
        signal_created_at: fixed_time(),
        evaluated_at: fixed_time(),
    }
}

fn approved_risk_evaluation(correlation_id: Uuid) -> RiskEvaluationResult {
    RiskEvaluationResult {
        risk_decision_id: Uuid::from_u128(0x301),
        decision: RiskEvaluationDecision::Approved,
        approved_notional: Some(Decimal::new(100_000, 0)),
        risk_score: Decimal::new(5, 1),
        reasons: Vec::new(),
        rule_results: vec![RiskRuleResult {
            rule_name: "kill_switch".to_string(),
            decision: RiskRuleDecision::Pass,
            reason: None,
            message: None,
        }],
        correlation_id,
    }
}

fn sample_order_intent(risk_decision_id: Uuid, idempotency_key: &str) -> OrderIntent {
    OrderIntent {
        order_id: Uuid::from_u128(0x401),
        correlation_id: Uuid::from_u128(0x501),
        risk_decision_id,
        idempotency_key: idempotency_key.to_string(),
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
        side: Side::Buy,
        quantity: Decimal::ONE,
        limit_price: Some(Decimal::new(100_000, 0)),
        created_at: fixed_time(),
        expires_at: None,
    }
}

fn sample_backtest_request() -> BacktestRequest {
    BacktestRequest {
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "1m".to_string(),
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 10, 0).unwrap(),
        initial_capital: Decimal::new(1_000_000, 0),
        risk_config_id: None,
        risk_config: None,
        fee_bps: Decimal::new(10, 0),
        slippage_bps: Decimal::new(5, 0),
        correlation_id: Some(Uuid::from_u128(0x901)),
        holding_candles: Some(3),
    }
}

fn sample_backtest_candle(index: i64, close: i64) -> Candle {
    let open_time =
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + chrono::Duration::minutes(index);
    Candle {
        id: Uuid::new_v4(),
        exchange: MarketDataSource::Binance,
        symbol: Symbol::new("BTCUSDT").unwrap(),
        interval: CandleInterval::OneMinute,
        open_time,
        close_time: open_time + chrono::Duration::minutes(1),
        open: Decimal::new(close - 1, 0),
        high: Decimal::new(close + 1, 0),
        low: Decimal::new(close - 2, 0),
        close: Decimal::new(close, 0),
        volume: Decimal::new(10, 0),
        quote_volume: Some(Decimal::new(1_000, 0)),
        trade_count: 1,
        is_closed: true,
        created_at: open_time,
        updated_at: open_time,
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn kill_switch_persists_across_sessions() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let correlation_id = Uuid::from_u128(0x601);

    set_kill_switch_state(
        &test_db.pool,
        &StateActor::system("integration-test"),
        correlation_id,
        "integration_db",
        true,
        Some("manual stop".to_string()),
    )
    .await
    .expect("kill switch should persist");

    let current = get_system_state(&test_db.pool)
        .await
        .expect("state should load");
    assert!(current.kill_switch_enabled);
    assert_eq!(current.kill_switch_reason.as_deref(), Some("manual stop"));

    let second_pool = db::connect_pool(&db::DbConfig::new(test_db.database_url.clone()))
        .await
        .expect("second pool should connect");
    let reloaded = get_system_state(&second_pool)
        .await
        .expect("state should reload");
    assert!(reloaded.kill_switch_enabled);
    assert_eq!(reloaded.kill_switch_reason.as_deref(), Some("manual stop"));
    assert_eq!(reloaded.last_correlation_id, correlation_id);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn approved_risk_decision_round_trips_with_rationale() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let signal = sample_signal();
    let inserted_signal = insert_signal_deduped(&test_db.pool, &signal)
        .await
        .expect("signal should persist");
    let correlation_id = Uuid::from_u128(0x701);
    let context = sample_risk_context(inserted_signal.signal.id, correlation_id);
    let evaluation = approved_risk_evaluation(correlation_id);

    let inserted = insert_risk_decision(&test_db.pool, "integration_db", &context, &evaluation)
        .await
        .expect("risk decision should persist");
    let loaded = get_risk_decision(&test_db.pool, inserted.risk_decision_id)
        .await
        .expect("risk decision query should succeed")
        .expect("risk decision should exist");

    assert_eq!(loaded.decision, "APPROVED");
    assert_eq!(loaded.signal_id, Some(inserted_signal.signal.id));
    assert_eq!(loaded.correlation_id, correlation_id);

    let rationale: Value = serde_json::from_str(&loaded.rationale).expect("valid JSON rationale");
    assert_eq!(rationale["strategy_id"], "momentum_v1");
    assert_eq!(rationale["symbol"], "BTCUSDT");
    assert_eq!(rationale["approved_notional"], "100000");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn duplicate_signal_is_deduped_cleanly() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let signal = sample_signal();

    let first = insert_signal_deduped(&test_db.pool, &signal)
        .await
        .expect("first insert should work");
    let second = insert_signal_deduped(&test_db.pool, &signal)
        .await
        .expect("second insert should resolve cleanly");

    assert!(first.inserted);
    assert!(!second.inserted);
    assert_eq!(first.signal.id, second.signal.id);

    let symbol = Symbol::new("BTCUSDT").expect("valid symbol");
    let signals = list_recent_signals(&test_db.pool, Some(&symbol), 10)
        .await
        .expect("signals should list");
    assert_eq!(signals.len(), 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn duplicate_idempotency_key_reuses_existing_order_safely() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let signal = sample_signal();
    let inserted_signal = insert_signal_deduped(&test_db.pool, &signal)
        .await
        .expect("signal should persist");
    let correlation_id = Uuid::from_u128(0x801);
    let context = sample_risk_context(inserted_signal.signal.id, correlation_id);
    let evaluation = approved_risk_evaluation(correlation_id);
    let risk = insert_risk_decision(&test_db.pool, "integration_db", &context, &evaluation)
        .await
        .expect("risk should persist");
    let idempotency_key = "momentum_v1:test-idempotency";

    let first = create_paper_order(
        &test_db.pool,
        "integration_db",
        &StateActor::system("integration-test"),
        sample_order_intent(risk.risk_decision_id, idempotency_key),
    )
    .await
    .expect("first order should persist");

    let duplicate = create_paper_order(
        &test_db.pool,
        "integration_db",
        &StateActor::system("integration-test"),
        sample_order_intent(risk.risk_decision_id, idempotency_key),
    )
    .await;

    match duplicate {
        Err(CreateOrderError::DuplicateIdempotencyKey) => {}
        other => panic!("expected duplicate idempotency error, got {other:?}"),
    }

    let existing = get_order_by_idempotency_key(&test_db.pool, idempotency_key)
        .await
        .expect("existing order query should work")
        .expect("existing order should be returned");
    assert_eq!(existing.order_id, first.order.order_id);

    let orders = list_orders(&test_db.pool)
        .await
        .expect("orders should list");
    assert_eq!(orders.len(), 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn backtest_tables_persist_and_round_trip() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let request = sample_backtest_request();
    let config = request.config();
    let run_id = Uuid::from_u128(0x902);
    let created_at = fixed_time();

    for close in [100_i64, 101, 102, 103] {
        let candle = sample_backtest_candle(close - 100, close);
        upsert_candle(&test_db.pool, &candle)
            .await
            .expect("candle should persist");
    }

    let inserted = insert_backtest_run(
        &test_db.pool,
        run_id,
        &request,
        &config,
        created_at,
        ReplayRunStatus::Running,
        request.correlation_id,
    )
    .await
    .expect("backtest run should persist");
    assert_eq!(inserted.id, run_id);

    let trade = BacktestTrade {
        id: Uuid::from_u128(0x903),
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        side: Side::Buy,
        entry_time: fixed_time(),
        entry_price: Decimal::new(100, 0),
        exit_time: Some(fixed_time()),
        exit_price: Some(Decimal::new(103, 0)),
        quantity: Decimal::ONE,
        notional: Decimal::new(100, 0),
        fee_paid: Decimal::new(1, 0),
        slippage_cost: Decimal::new(1, 0),
        realized_pnl: Decimal::new(2, 0),
        reason: "holding_period".to_string(),
        created_at,
    };
    insert_backtest_trade(&test_db.pool, &trade)
        .await
        .expect("backtest trade should persist");

    insert_backtest_equity_points(
        &test_db.pool,
        &[BacktestEquityPoint {
            id: Uuid::from_u128(0x904),
            run_id,
            timestamp: fixed_time(),
            equity: Decimal::new(1_000_002, 0),
            drawdown_pct: Decimal::new(25, 2),
        }],
    )
    .await
    .expect("equity curve should persist");

    let completed = BacktestResult {
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        final_equity: Decimal::new(1_000_002, 0),
        pnl: Decimal::new(2, 0),
        pnl_pct: Decimal::new(2, 4),
        max_drawdown_pct: Decimal::new(25, 2),
        win_rate: Decimal::new(100, 0),
        trade_count: 1,
        winning_trades: 1,
        losing_trades: 0,
        avg_win: Decimal::new(2, 0),
        avg_loss: Decimal::ZERO,
        fee_paid: Decimal::new(1, 0),
        slippage_cost: Decimal::new(1, 0),
        status: ReplayRunStatus::Completed,
        created_at,
        correlation_id: request.correlation_id,
    };
    update_backtest_run_completed(&test_db.pool, &completed, &config)
        .await
        .expect("backtest run should update");

    let loaded_run = get_backtest_run(&test_db.pool, run_id)
        .await
        .expect("run query should succeed")
        .expect("run should exist");
    let loaded_trades = get_backtest_trades(&test_db.pool, run_id)
        .await
        .expect("trade query should succeed");
    let loaded_equity = get_backtest_equity_curve(&test_db.pool, run_id)
        .await
        .expect("equity query should succeed");
    let loaded_candles = get_closed_candles_range(
        &test_db.pool,
        &Symbol::new("BTCUSDT").unwrap(),
        CandleInterval::OneMinute,
        request.start_time,
        request.end_time,
    )
    .await
    .expect("candle range query should succeed");

    assert_eq!(loaded_run.status, "COMPLETED");
    assert_eq!(loaded_trades.len(), 1);
    assert_eq!(loaded_equity.len(), 1);
    assert_eq!(loaded_candles.len(), 4);
}
