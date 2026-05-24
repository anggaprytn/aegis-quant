use aegis_core::{
    CandleInterval, OrderIntent, RiskCheckContext, RiskEvaluationDecision, RiskEvaluationResult,
    RiskRuleDecision, RiskRuleResult, Side, SignalConfidence, SignalReason, SignalSide, StrategyId,
    StrategySignal, Symbol,
};
use chrono::{TimeZone, Utc};
use db::{
    create_paper_order, get_order_by_idempotency_key, get_risk_decision, get_system_state,
    insert_risk_decision, insert_signal_deduped, list_orders, list_recent_signals,
    set_kill_switch_state, test_support::TestDatabase, CreateOrderError, StateActor,
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
