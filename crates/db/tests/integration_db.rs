use aegis_core::{
    aggregate_closed_1m_candles, BacktestConfig, BacktestEquityPoint, BacktestRequest,
    BacktestResult, BacktestTrade, Candle, CandleBackfillRequest, CandleBackfillStatus,
    CandleInterval, ExchangeEnvironment, ExchangeExecutionReport, ExchangeExecutionReportType,
    ExchangeExecutionStatus, ExchangeName, ExchangeOrderSide, ExchangeOrderState,
    ExchangeOrderStatus, ExchangeOrderTimeInForce, ExchangeOrderType, ExchangeReconciliationAction,
    ExchangeReconciliationMismatchKind, ExchangeReconciliationSummary, FeeModel, MarketDataSource,
    OrderIntent, PaperAccount, PaperAccountStatus, PaperPosition, PaperPriceStatus, PositionSide,
    PositionStatus, ReplayMode, ReplayRunStatus, RiskCheckContext, RiskEvaluationDecision,
    RiskEvaluationResult, RiskRuleDecision, RiskRuleResult, Side, SignalConfidence, SignalReason,
    SignalSide, StrategyExperimentCandidate, StrategyExperimentComparison,
    StrategyExperimentMetric, StrategyExperimentResult, StrategyExperimentRun,
    StrategyExperimentStatus, StrategyId, StrategyPerformanceMode, StrategyPerformanceRequest,
    StrategySignal, StrategyWalkForwardCandidate, StrategyWalkForwardRequest,
    StrategyWalkForwardResult, StrategyWalkForwardRobustnessSummary, StrategyWalkForwardStatus,
    StrategyWalkForwardWindow, StrategyWalkForwardWindowResult, Symbol, TestnetExecutionState,
    TestnetExecutionTransitionSource, TestnetPromotionFunnelRequest, TestnetShadowRunnerConfig,
    TestnetShadowRunnerStaleFeedPolicy, TestnetShadowRunnerStatus,
};
use chrono::{TimeZone, Utc};
use db::{
    append_exchange_testnet_lifecycle_event_and_update_order, count_candles_by_interval,
    count_candles_range, create_paper_order, fail_exchange_reconciliation_run,
    get_aggregated_candle_coverage, get_backtest_equity_curve, get_backtest_run,
    get_backtest_trades, get_candle_backfill_run, get_closed_1m_candles_range,
    get_closed_candles_range, get_exchange_private_stream_state, get_exchange_reconciliation_run,
    get_exchange_testnet_order_by_client_order_id, get_order_by_idempotency_key, get_risk_decision,
    get_strategy_paper_pnl_breakdown, get_strategy_performance_summary,
    get_strategy_shadow_decision_breakdown, get_system_state, get_testnet_promotion_funnel_summary,
    get_testnet_promotion_lifecycle_breakdown, insert_backtest_equity_points, insert_backtest_run,
    insert_backtest_trade, insert_candle_backfill_run, insert_exchange_private_stream_event,
    insert_exchange_reconciliation_mismatch, insert_exchange_reconciliation_run,
    insert_exchange_testnet_order, insert_exchange_testnet_order_lifecycle_event,
    insert_paper_account, insert_risk_decision, insert_signal_deduped, insert_strategy_experiment,
    insert_strategy_experiment_runs, insert_strategy_walk_forward_run,
    insert_strategy_walk_forward_windows, insert_testnet_shadow_promotion,
    insert_testnet_shadow_run, list_exchange_private_stream_events,
    list_exchange_reconciliation_mismatches, list_exchange_testnet_order_lifecycle_events,
    list_orders, list_recent_signals, list_strategy_experiment_runs, list_strategy_experiments,
    list_strategy_performance_rankings, list_strategy_walk_forward_runs,
    list_strategy_walk_forward_windows, list_testnet_promotion_funnel_rows, set_kill_switch_state,
    strategy_experiment_result_from_records, strategy_walk_forward_result_from_records,
    strategy_walk_forward_window_from_record, test_support::TestDatabase,
    testnet_shadow_runner_config_from_record, testnet_shadow_runner_state_from_record,
    update_backtest_run_completed, update_exchange_testnet_order_status, upsert_aggregated_candles,
    upsert_candle, upsert_candles_batch, upsert_exchange_private_stream_state,
    upsert_paper_position, upsert_testnet_shadow_runner_config, upsert_testnet_shadow_runner_state,
    CreateOrderError, ExchangePrivateStreamEventRecord, ExchangePrivateStreamStateRecord,
    ExchangeReconciliationMismatchRecord, ExchangeReconciliationRunRecord,
    ExchangeTestnetOrderLifecycleEventRecord, ExchangeTestnetOrderRecord, StateActor,
    TestnetShadowPromotionRecord, TestnetShadowRunRecord, TESTNET_SHADOW_RUNNER_CONFIG_ID,
    TESTNET_SHADOW_RUNNER_STATE_ID,
};
use exchange::{
    apply_testnet_transition, local_testnet_order_status_from_private_execution_report,
    map_private_execution_report_to_transition, map_rest_reconciliation_status_to_transition,
};
use rust_decimal::Decimal;
use serde_json::{json, Value};
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
        strategy_config_override: None,
    }
}

fn sample_backtest_config() -> BacktestConfig {
    BacktestConfig {
        replay_mode: ReplayMode::Backtest,
        holding_candles: 3,
        fee_model: FeeModel::Bps,
        slippage_model: aegis_core::SlippageModel::Bps,
        fee_bps: Decimal::new(10, 0),
        slippage_bps: Decimal::new(5, 0),
        risk_config_id: None,
        risk_config: None,
    }
}

fn sample_paper_account() -> PaperAccount {
    PaperAccount {
        id: Uuid::new_v4(),
        name: "paper-main".to_string(),
        base_currency: "USDT".to_string(),
        initial_equity: Decimal::new(1_000_000, 0),
        current_equity: Decimal::new(1_010_000, 0),
        realized_pnl: Decimal::new(10_000, 0),
        unrealized_pnl: Decimal::ZERO,
        status: PaperAccountStatus::Active,
        created_at: fixed_time(),
        updated_at: fixed_time(),
    }
}

fn sample_strategy_experiment_run(
    experiment_id: Uuid,
    rank: i32,
    score: i64,
) -> StrategyExperimentRun {
    StrategyExperimentRun {
        id: Uuid::new_v4(),
        experiment_id,
        rank,
        candidate: StrategyExperimentCandidate {
            lookback_candles: 3 + rank as u32,
            holding_candles: Some(3),
            stop_loss_pct: None,
            take_profit_pct: None,
            max_signal_age_ms: Some(180_000),
        },
        final_equity: Decimal::new(1_000_000 + score * 10_000, 0),
        pnl: Decimal::new(score * 10_000, 0),
        pnl_pct: Decimal::new(score, 0),
        max_drawdown_pct: Decimal::new(2 * rank as i64, 0),
        win_rate: Decimal::new(50 + rank as i64, 0),
        trade_count: 5 * rank,
        fee_paid: Decimal::new(100, 0),
        slippage_cost: Decimal::new(50, 0),
        fee_slippage_drag_pct: Decimal::new(15, 2),
        score: Decimal::new(score, 0),
        status: StrategyExperimentStatus::Completed,
        warnings: Vec::new(),
        created_at: fixed_time(),
    }
}

fn sample_strategy_experiment_result(
    experiment_id: Uuid,
    runs: &[StrategyExperimentRun],
) -> StrategyExperimentResult {
    StrategyExperimentResult {
        experiment_id,
        experiment_group_id: None,
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "1m".to_string(),
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        initial_capital: Decimal::new(1_000_000, 0),
        fee_bps: Decimal::new(10, 0),
        slippage_bps: Decimal::new(5, 0),
        max_signal_age_ms: Some(180_000),
        max_runs: Some(runs.len() as u32),
        status: StrategyExperimentStatus::Completed,
        run_count: runs.len() as i32,
        comparison: StrategyExperimentComparison {
            ranking_metric: StrategyExperimentMetric::RiskAdjustedScore,
            best_run_id: runs.first().map(|run| run.id),
            worst_run_id: runs.last().map(|run| run.id),
            ranked_run_ids: runs.iter().map(|run| run.id).collect(),
        },
        best_run: runs.first().cloned(),
        worst_run: runs.last().cloned(),
        candle_count: Some(120),
        warnings: Vec::new(),
        skipped_reason: None,
        created_at: fixed_time(),
        correlation_id: Some(Uuid::from_u128(0xefe)),
    }
}

fn sample_strategy_walk_forward_request() -> StrategyWalkForwardRequest {
    StrategyWalkForwardRequest {
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 5, 24, 0, 0, 0).unwrap(),
        window_train_size_hours: 72,
        window_test_size_hours: 24,
        step_size_hours: 24,
        initial_capital: Decimal::new(1_000_000, 0),
        fee_bps: Decimal::new(10, 0),
        slippage_bps: Decimal::new(5, 0),
        candidate_config: StrategyWalkForwardCandidate {
            lookback_candles: 5,
            holding_candles: Some(3),
            stop_loss_pct: None,
            take_profit_pct: None,
            max_signal_age_ms: Some(180_000),
        },
        min_required_test_windows: Some(2),
        correlation_id: Some(Uuid::from_u128(0x9901)),
    }
}

fn sample_strategy_walk_forward_result(walk_forward_id: Uuid) -> StrategyWalkForwardResult {
    StrategyWalkForwardResult {
        walk_forward_id,
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        total_windows: 3,
        completed_windows: 2,
        skipped_windows: 1,
        profitable_test_windows: 1,
        losing_test_windows: 1,
        avg_test_pnl_pct: Decimal::new(15, 1),
        median_test_pnl_pct: Decimal::new(15, 1),
        worst_test_pnl_pct: Decimal::new(-1, 0),
        best_test_pnl_pct: Decimal::new(4, 0),
        avg_max_drawdown_pct: Decimal::new(25, 1),
        robustness_score: Decimal::new(42, 1),
        status: StrategyWalkForwardStatus::Completed,
        robustness_summary: StrategyWalkForwardRobustnessSummary {
            profitable_window_pct: Decimal::new(50, 0),
            total_trade_count: 9,
            avg_trades_per_completed_window: Decimal::new(45, 1),
            avg_fee_slippage_drag_pct: Decimal::new(15, 2),
            skipped_window_pct: Decimal::new(3333, 2),
            dominant_winner_share_pct: Decimal::new(60, 0),
        },
        created_at: fixed_time(),
        correlation_id: Some(Uuid::from_u128(0x9902)),
    }
}

fn sample_strategy_walk_forward_windows(
    walk_forward_id: Uuid,
) -> Vec<StrategyWalkForwardWindowResult> {
    vec![
        StrategyWalkForwardWindowResult {
            id: Uuid::from_u128(0x9911),
            walk_forward_id,
            window: StrategyWalkForwardWindow {
                window_index: 0,
                train_start: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
                train_end: Utc.with_ymd_and_hms(2026, 5, 4, 0, 0, 0).unwrap(),
                test_start: Utc.with_ymd_and_hms(2026, 5, 4, 0, 0, 0).unwrap(),
                test_end: Utc.with_ymd_and_hms(2026, 5, 5, 0, 0, 0).unwrap(),
            },
            status: StrategyWalkForwardStatus::Completed,
            skip_reason: None,
            trade_count: 5,
            pnl: Decimal::new(40_000, 0),
            pnl_pct: Decimal::new(4, 0),
            max_drawdown_pct: Decimal::new(2, 0),
            win_rate: Decimal::new(60, 0),
            fee_paid: Decimal::new(1_000, 0),
            slippage_cost: Decimal::new(500, 0),
            result: json!({ "status": "COMPLETED" }),
            created_at: fixed_time(),
        },
        StrategyWalkForwardWindowResult {
            id: Uuid::from_u128(0x9912),
            walk_forward_id,
            window: StrategyWalkForwardWindow {
                window_index: 1,
                train_start: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
                train_end: Utc.with_ymd_and_hms(2026, 5, 5, 0, 0, 0).unwrap(),
                test_start: Utc.with_ymd_and_hms(2026, 5, 5, 0, 0, 0).unwrap(),
                test_end: Utc.with_ymd_and_hms(2026, 5, 6, 0, 0, 0).unwrap(),
            },
            status: StrategyWalkForwardStatus::Completed,
            skip_reason: None,
            trade_count: 4,
            pnl: Decimal::new(-10_000, 0),
            pnl_pct: Decimal::new(-1, 0),
            max_drawdown_pct: Decimal::new(3, 0),
            win_rate: Decimal::new(25, 0),
            fee_paid: Decimal::new(900, 0),
            slippage_cost: Decimal::new(400, 0),
            result: json!({ "status": "COMPLETED" }),
            created_at: fixed_time(),
        },
        StrategyWalkForwardWindowResult {
            id: Uuid::from_u128(0x9913),
            walk_forward_id,
            window: StrategyWalkForwardWindow {
                window_index: 2,
                train_start: Utc.with_ymd_and_hms(2026, 5, 3, 0, 0, 0).unwrap(),
                train_end: Utc.with_ymd_and_hms(2026, 5, 6, 0, 0, 0).unwrap(),
                test_start: Utc.with_ymd_and_hms(2026, 5, 6, 0, 0, 0).unwrap(),
                test_end: Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap(),
            },
            status: StrategyWalkForwardStatus::Skipped,
            skip_reason: Some(
                "insufficient_candle_coverage: expected=96 actual=80 required=10".to_string(),
            ),
            trade_count: 0,
            pnl: Decimal::ZERO,
            pnl_pct: Decimal::ZERO,
            max_drawdown_pct: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            fee_paid: Decimal::ZERO,
            slippage_cost: Decimal::ZERO,
            result: json!({ "status": "SKIPPED" }),
            created_at: fixed_time(),
        },
    ]
}

fn sample_paper_position(account_id: Uuid) -> PaperPosition {
    PaperPosition {
        id: Uuid::new_v4(),
        account_id,
        symbol: "BTCUSDT".to_string(),
        side: PositionSide::Long,
        quantity: Decimal::ONE,
        entry_price: Decimal::new(100_000, 0),
        mark_price: Some(Decimal::new(101_000, 0)),
        price_status: PaperPriceStatus::Live,
        notional: Decimal::new(100_000, 0),
        realized_pnl: Decimal::new(5_000, 0),
        unrealized_pnl: Decimal::ZERO,
        status: PositionStatus::Closed,
        opened_at: fixed_time(),
        closed_at: Some(fixed_time() + chrono::Duration::minutes(5)),
        strategy_id: Some("momentum_v1".to_string()),
        signal_id: None,
        risk_decision_id: None,
        order_id: None,
        updated_at: fixed_time() + chrono::Duration::minutes(5),
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

fn sample_backfill_request() -> CandleBackfillRequest {
    CandleBackfillRequest {
        exchange: MarketDataSource::Binance,
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 10, 0).unwrap(),
        limit_per_request: Some(1000),
        correlation_id: Some(Uuid::from_u128(0xaaaa)),
    }
}

fn sample_exchange_testnet_order_record() -> ExchangeTestnetOrderRecord {
    ExchangeTestnetOrderRecord {
        id: Uuid::new_v4(),
        exchange: "binance".to_string(),
        environment: "testnet".to_string(),
        client_order_id: format!("aegis-testnet-{}", Uuid::new_v4()),
        exchange_order_id: Some("123".to_string()),
        symbol: "BTCUSDT".to_string(),
        side: "BUY".to_string(),
        order_type: "LIMIT".to_string(),
        time_in_force: Some("GTC".to_string()),
        requested_qty: Some(Decimal::ONE),
        requested_notional: None,
        limit_price: Some(Decimal::new(100_000, 0)),
        status: "NEW".to_string(),
        execution_state: "NEW".to_string(),
        ack_payload: Some(json!({"status":"NEW"})),
        latest_status_payload: Some(json!({"status":"NEW"})),
        risk_decision_id: None,
        created_by: None,
        last_transition_at: Some(fixed_time()),
        created_at: fixed_time(),
        updated_at: fixed_time(),
    }
}

fn lifecycle_time(offset_seconds: i64) -> chrono::DateTime<Utc> {
    fixed_time() + chrono::Duration::seconds(offset_seconds)
}

fn sample_exchange_testnet_order_record_with_state(
    status: &str,
    execution_state: TestnetExecutionState,
) -> ExchangeTestnetOrderRecord {
    let mut record = sample_exchange_testnet_order_record();
    record.status = status.to_string();
    record.execution_state = execution_state.as_str().to_string();
    record.ack_payload = Some(json!({ "status": status }));
    record.latest_status_payload = Some(json!({ "status": status }));
    record.last_transition_at = Some(fixed_time());
    record
}

fn lifecycle_event_record(
    order: &ExchangeTestnetOrderRecord,
    previous_state: Option<TestnetExecutionState>,
    next_state: TestnetExecutionState,
    source: TestnetExecutionTransitionSource,
    reason: Option<&str>,
    payload: Option<Value>,
    created_at: chrono::DateTime<Utc>,
) -> ExchangeTestnetOrderLifecycleEventRecord {
    ExchangeTestnetOrderLifecycleEventRecord {
        id: Uuid::new_v4(),
        order_id: Some(order.id),
        client_order_id: order.client_order_id.clone(),
        previous_state: previous_state.map(|value| value.as_str().to_string()),
        next_state: next_state.as_str().to_string(),
        transition_source: source.as_str().to_string(),
        reason: reason.map(ToString::to_string),
        payload,
        created_by: None,
        created_at,
        correlation_id: None,
    }
}

async fn append_lifecycle_event(
    pool: &db::PgPool,
    event: &ExchangeTestnetOrderLifecycleEventRecord,
    exchange_order_id: Option<&str>,
    status: Option<&str>,
    execution_state: TestnetExecutionState,
    latest_status_payload: Option<&Value>,
    ack_payload: Option<&Value>,
) -> ExchangeTestnetOrderRecord {
    append_exchange_testnet_lifecycle_event_and_update_order(
        pool,
        event,
        exchange_order_id,
        status,
        execution_state,
        latest_status_payload,
        ack_payload,
    )
    .await
    .expect("lifecycle event should persist")
    .expect("testnet order should exist")
}

async fn insert_promotion_funnel_fixture(
    pool: &db::PgPool,
    symbol: &str,
    promotion_status: &str,
    execution_state: Option<TestnetExecutionState>,
    delete_linked_order_after_insert: bool,
) -> (
    TestnetShadowRunRecord,
    TestnetShadowPromotionRecord,
    Option<ExchangeTestnetOrderRecord>,
) {
    let shadow_run = TestnetShadowRunRecord {
        id: Uuid::new_v4(),
        strategy_id: "momentum_v1".to_string(),
        symbol: symbol.to_string(),
        timeframe: "1m".to_string(),
        decision: "WOULD_SUBMIT".to_string(),
        signal_id: None,
        risk_decision_id: None,
        would_submit_payload: Some(json!({"symbol": symbol, "side": "BUY"})),
        price_source: Some("stored_tick".to_string()),
        resolved_price: Some(Decimal::new(100_000, 0)),
        reasons: Vec::new(),
        status: "COMPLETED".to_string(),
        created_at: fixed_time(),
        correlation_id: Some(Uuid::new_v4()),
    };
    insert_testnet_shadow_run(pool, &shadow_run)
        .await
        .expect("shadow run should persist");

    let maybe_order = execution_state.map(|state| {
        let mut order = sample_exchange_testnet_order_record_with_state(state.as_str(), state);
        order.symbol = symbol.to_string();
        order.client_order_id = format!("aegis-promo-{symbol}-{}", Uuid::new_v4());
        order.created_at = fixed_time() + chrono::Duration::seconds(2);
        order.updated_at = order.created_at;
        order.last_transition_at = Some(order.created_at);
        order
    });

    if let Some(order) = maybe_order.as_ref() {
        insert_exchange_testnet_order(pool, order)
            .await
            .expect("testnet order should persist");
        insert_exchange_testnet_order_lifecycle_event(
            pool,
            &lifecycle_event_record(
                order,
                None,
                TestnetExecutionState::ExchangeAcked,
                TestnetExecutionTransitionSource::ExchangeAck,
                Some("acked"),
                Some(json!({"status":"NEW"})),
                lifecycle_time(3),
            ),
        )
        .await
        .expect("acked lifecycle should persist");
        if execution_state != Some(TestnetExecutionState::ExchangeAcked) {
            insert_exchange_testnet_order_lifecycle_event(
                pool,
                &lifecycle_event_record(
                    order,
                    Some(TestnetExecutionState::ExchangeAcked),
                    execution_state.expect("execution state should exist"),
                    TestnetExecutionTransitionSource::PrivateStream,
                    Some("terminal"),
                    Some(json!({"status": execution_state.expect("execution state").as_str()})),
                    lifecycle_time(4),
                ),
            )
            .await
            .expect("terminal lifecycle should persist");
        }
    }

    let promotion = TestnetShadowPromotionRecord {
        id: Uuid::new_v4(),
        shadow_run_id: shadow_run.id,
        status: promotion_status.to_string(),
        strategy_id: Some("momentum_v1".to_string()),
        symbol: Some(symbol.to_string()),
        timeframe: Some("1m".to_string()),
        signal_id: None,
        risk_decision_id: None,
        would_submit_payload: json!({"symbol": symbol, "side": "BUY"}),
        resolved_price: Some(Decimal::new(100_000, 0)),
        price_source: Some("stored_tick".to_string()),
        rejection_reasons: if promotion_status == "REJECTED" {
            vec!["submit_failed".to_string()]
        } else {
            Vec::new()
        },
        testnet_order_id: maybe_order.as_ref().map(|order| order.id),
        client_order_id: maybe_order
            .as_ref()
            .map(|order| order.client_order_id.clone()),
        expires_at: fixed_time() + chrono::Duration::minutes(5),
        created_by: None,
        submitted_by: None,
        created_at: fixed_time() + chrono::Duration::seconds(1),
        submitted_at: if promotion_status == "PREVIEWED" {
            None
        } else {
            Some(fixed_time() + chrono::Duration::seconds(2))
        },
        correlation_id: Some(Uuid::new_v4()),
    };
    insert_testnet_shadow_promotion(pool, &promotion)
        .await
        .expect("promotion should persist");

    if delete_linked_order_after_insert {
        sqlx::query("DELETE FROM exchange_testnet_orders WHERE id = $1")
            .bind(promotion.testnet_order_id)
            .execute(pool)
            .await
            .expect("testnet order delete should succeed");
    }

    (shadow_run, promotion, maybe_order)
}

fn sample_private_execution_report(
    order: &ExchangeTestnetOrderRecord,
    order_status: ExchangeExecutionStatus,
    execution_type: ExchangeExecutionReportType,
    raw_payload: Value,
) -> ExchangeExecutionReport {
    ExchangeExecutionReport {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: order.symbol.clone(),
        client_order_id: order.client_order_id.clone(),
        exchange_order_id: order.exchange_order_id.clone(),
        side: ExchangeOrderSide::Buy,
        order_type: ExchangeOrderType::Limit,
        time_in_force: Some(ExchangeOrderTimeInForce::Gtc),
        order_status,
        execution_type,
        last_executed_qty: Decimal::new(2, 1),
        cumulative_filled_qty: Decimal::new(5, 1),
        last_executed_price: Decimal::new(100_500, 0),
        commission_amount: None,
        commission_asset: None,
        event_time: lifecycle_time(10),
        transaction_time: Some(lifecycle_time(10)),
        raw_payload,
    }
}

async fn apply_private_stream_report(
    pool: &db::PgPool,
    report: &ExchangeExecutionReport,
) -> ExchangeTestnetOrderRecord {
    let order = get_exchange_testnet_order_by_client_order_id(pool, &report.client_order_id)
        .await
        .expect("order query should succeed")
        .expect("testnet order should exist");
    let (next_state, reason) = map_private_execution_report_to_transition(report);
    let payload = report.raw_payload.clone();
    let transition = apply_testnet_transition(
        &aegis_core::TestnetOrderLifecycleSnapshot {
            order_id: Some(order.id),
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
            current_state: order
                .execution_state
                .parse()
                .expect("execution_state should parse"),
            last_transition_at: order.last_transition_at,
        },
        next_state,
        TestnetExecutionTransitionSource::PrivateStream,
        reason.map(ToString::to_string),
        Some(payload.clone()),
    )
    .expect("private stream transition should be valid");

    append_lifecycle_event(
        pool,
        &ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(order.id),
            client_order_id: order.client_order_id.clone(),
            previous_state: transition
                .previous_state
                .map(|value| value.as_str().to_string()),
            next_state: transition.next_state.as_str().to_string(),
            transition_source: transition.source.as_str().to_string(),
            reason: transition.reason,
            payload: transition.payload.clone(),
            created_by: None,
            created_at: lifecycle_time(10),
            correlation_id: None,
        },
        report.exchange_order_id.as_deref(),
        Some(local_testnet_order_status_from_private_execution_report(
            report,
        )),
        transition.next_state,
        Some(&payload),
        None,
    )
    .await
}

async fn apply_rest_reconciliation_status(
    pool: &db::PgPool,
    order: &ExchangeTestnetOrderRecord,
    status: &ExchangeOrderStatus,
) -> ExchangeTestnetOrderRecord {
    let persisted_order =
        get_exchange_testnet_order_by_client_order_id(pool, &order.client_order_id)
            .await
            .expect("order query should succeed")
            .expect("testnet order should exist");
    let persisted_status = match status.status {
        ExchangeOrderState::New => "NEW",
        ExchangeOrderState::PartiallyFilled => "PARTIALLY_FILLED",
        ExchangeOrderState::Filled => "FILLED",
        ExchangeOrderState::Canceled => "CANCELLED",
        ExchangeOrderState::Rejected => "REJECTED",
        ExchangeOrderState::Expired => "EXPIRED",
        ExchangeOrderState::PendingCancel => "PENDING_CANCEL",
    };
    let (next_state, reason) = map_rest_reconciliation_status_to_transition(status);
    let payload = status.raw_payload.clone();
    let event = match apply_testnet_transition(
        &aegis_core::TestnetOrderLifecycleSnapshot {
            order_id: Some(persisted_order.id),
            client_order_id: persisted_order.client_order_id.clone(),
            exchange_order_id: persisted_order.exchange_order_id.clone(),
            current_state: persisted_order
                .execution_state
                .parse()
                .expect("execution_state should parse"),
            last_transition_at: persisted_order.last_transition_at,
        },
        next_state,
        TestnetExecutionTransitionSource::RestReconciliation,
        reason.map(ToString::to_string),
        Some(payload.clone()),
    ) {
        Ok(transition) => ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(persisted_order.id),
            client_order_id: persisted_order.client_order_id.clone(),
            previous_state: transition
                .previous_state
                .map(|value| value.as_str().to_string()),
            next_state: transition.next_state.as_str().to_string(),
            transition_source: transition.source.as_str().to_string(),
            reason: transition.reason,
            payload: transition.payload.clone(),
            created_by: None,
            created_at: lifecycle_time(20),
            correlation_id: None,
        },
        Err(_) => ExchangeTestnetOrderLifecycleEventRecord {
            id: Uuid::new_v4(),
            order_id: Some(persisted_order.id),
            client_order_id: persisted_order.client_order_id.clone(),
            previous_state: Some(persisted_order.execution_state.clone()),
            next_state: TestnetExecutionState::ReconciliationRequired
                .as_str()
                .to_string(),
            transition_source: TestnetExecutionTransitionSource::RestReconciliation
                .as_str()
                .to_string(),
            reason: Some("invalid_rest_reconciliation_transition".to_string()),
            payload: Some(payload.clone()),
            created_by: None,
            created_at: lifecycle_time(20),
            correlation_id: None,
        },
    };
    let execution_state = event
        .next_state
        .parse()
        .expect("lifecycle execution_state should parse");

    append_lifecycle_event(
        pool,
        &event,
        status.exchange_order_id.as_deref(),
        Some(persisted_status),
        execution_state,
        Some(&payload),
        None,
    )
    .await
}

fn sample_exchange_order_status(
    order: &ExchangeTestnetOrderRecord,
    status: ExchangeOrderState,
    raw_payload: Value,
) -> ExchangeOrderStatus {
    ExchangeOrderStatus {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: order.symbol.clone(),
        client_order_id: order.client_order_id.clone(),
        exchange_order_id: order.exchange_order_id.clone(),
        status,
        side: ExchangeOrderSide::Buy,
        order_type: ExchangeOrderType::Limit,
        time_in_force: Some(ExchangeOrderTimeInForce::Gtc),
        original_qty: order.requested_qty,
        executed_qty: Decimal::new(5, 1),
        cumulative_quote_qty: Decimal::new(50_250, 0),
        limit_price: order.limit_price,
        updated_at: lifecycle_time(20),
        raw_payload,
    }
}

fn sample_exchange_reconciliation_run_record() -> ExchangeReconciliationRunRecord {
    ExchangeReconciliationRunRecord {
        id: Uuid::new_v4(),
        exchange: "binance".to_string(),
        environment: "testnet".to_string(),
        status: "RUNNING".to_string(),
        checked_orders: 0,
        matched_orders: 0,
        mismatched_orders: 0,
        unknown_orders: 0,
        failed_reason: None,
        correlation_id: Uuid::new_v4(),
        started_at: fixed_time(),
        completed_at: None,
    }
}

fn sample_private_stream_state_record() -> ExchangePrivateStreamStateRecord {
    ExchangePrivateStreamStateRecord {
        exchange: "binance".to_string(),
        environment: "testnet".to_string(),
        status: "CONNECTED".to_string(),
        listen_key_hash: Some("hash123".to_string()),
        connected_at: Some(fixed_time()),
        last_event_at: Some(fixed_time()),
        last_error: None,
        reconnect_count: 1,
        updated_at: fixed_time(),
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
async fn exchange_reconciliation_run_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let run = sample_exchange_reconciliation_run_record();

    let inserted = insert_exchange_reconciliation_run(&test_db.pool, &run)
        .await
        .expect("run should persist");
    let loaded = get_exchange_reconciliation_run(&test_db.pool, inserted.id)
        .await
        .expect("run query should succeed")
        .expect("run should exist");

    assert_eq!(loaded.id, inserted.id);
    assert_eq!(loaded.status, "RUNNING");
    assert_eq!(loaded.environment, "testnet");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn exchange_reconciliation_mismatch_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record();
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("order should persist");
    let run = insert_exchange_reconciliation_run(
        &test_db.pool,
        &sample_exchange_reconciliation_run_record(),
    )
    .await
    .expect("run should persist");

    insert_exchange_reconciliation_mismatch(
        &test_db.pool,
        &ExchangeReconciliationMismatchRecord {
            id: Uuid::new_v4(),
            run_id: run.id,
            client_order_id: order.client_order_id.clone(),
            local_status: Some("NEW".to_string()),
            exchange_status: Some("FILLED".to_string()),
            mismatch_kind: "STATUS_MISMATCH".to_string(),
            action: "UPDATE_LOCAL_STATUS".to_string(),
            payload: json!({ "reason": "test" }),
            created_at: fixed_time(),
        },
    )
    .await
    .expect("mismatch should persist");

    let mismatches = list_exchange_reconciliation_mismatches(&test_db.pool, run.id)
        .await
        .expect("mismatch query should succeed");

    assert_eq!(mismatches.len(), 1);
    assert_eq!(mismatches[0].client_order_id, order.client_order_id);
    assert_eq!(mismatches[0].mismatch_kind, "STATUS_MISMATCH");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn failed_exchange_reconciliation_run_persists_failed_reason() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let run = insert_exchange_reconciliation_run(
        &test_db.pool,
        &sample_exchange_reconciliation_run_record(),
    )
    .await
    .expect("run should persist");

    let summary = ExchangeReconciliationSummary {
        checked_orders: 1,
        matched_orders: 0,
        mismatched_orders: 0,
        unknown_orders: 0,
    };
    fail_exchange_reconciliation_run(&test_db.pool, run.id, &summary, "transport failure")
        .await
        .expect("run failure should persist");

    let loaded = get_exchange_reconciliation_run(&test_db.pool, run.id)
        .await
        .expect("run query should succeed")
        .expect("run should exist");
    assert_eq!(loaded.status, "FAILED");
    assert_eq!(loaded.failed_reason.as_deref(), Some("transport failure"));
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
async fn candle_backfill_run_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let request = sample_backfill_request();
    let run_id = Uuid::new_v4();

    let inserted = insert_candle_backfill_run(
        &test_db.pool,
        run_id,
        &request,
        request.correlation_id.unwrap(),
        fixed_time(),
        serde_json::to_value(&request).unwrap(),
    )
    .await
    .expect("backfill run should persist");

    assert_eq!(inserted.id, run_id);
    assert_eq!(inserted.status, CandleBackfillStatus::Running.as_str());

    let loaded = get_candle_backfill_run(&test_db.pool, run_id)
        .await
        .expect("query should work")
        .expect("backfill run should exist");
    assert_eq!(loaded.id, run_id);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candle_upsert_batch_is_idempotent() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let candle = sample_backtest_candle(0, 100);

    let first = upsert_candles_batch(&test_db.pool, std::slice::from_ref(&candle))
        .await
        .expect("first upsert should work");
    let second = upsert_candles_batch(&test_db.pool, std::slice::from_ref(&candle))
        .await
        .expect("second upsert should work");

    assert_eq!(first.inserted_candles, 1);
    assert_eq!(second.skipped_candles, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candle_count_range_returns_closed_candle_count() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let first = sample_backtest_candle(0, 100);
    let second = sample_backtest_candle(1, 101);
    upsert_candle(&test_db.pool, &first)
        .await
        .expect("first persists");
    upsert_candle(&test_db.pool, &second)
        .await
        .expect("second persists");

    let count = count_candles_range(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
        CandleInterval::OneMinute,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 3, 0).unwrap(),
    )
    .await
    .expect("count should work");

    assert_eq!(count, 2);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn aggregates_persisted_1m_candles_into_5m_idempotently() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    for minute in 0..5 {
        let candle = sample_backtest_candle(minute, 100 + minute);
        upsert_candle(&test_db.pool, &candle)
            .await
            .expect("1m candle should persist");
    }

    let source = get_closed_1m_candles_range(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 5, 0).unwrap(),
    )
    .await
    .expect("1m range should load");
    let aggregated = aggregate_closed_1m_candles(&source, CandleInterval::FiveMinutes);

    let first = upsert_aggregated_candles(&test_db.pool, &aggregated.candles)
        .await
        .expect("first aggregated upsert should work");
    let second = upsert_aggregated_candles(&test_db.pool, &aggregated.candles)
        .await
        .expect("second aggregated upsert should work");

    assert_eq!(aggregated.candles.len(), 1);
    assert_eq!(first.inserted_candles, 1);
    assert_eq!(second.skipped_candles, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candle_coverage_counts_multiple_intervals() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    for minute in 0..5 {
        let candle = sample_backtest_candle(minute, 100 + minute);
        upsert_candle(&test_db.pool, &candle)
            .await
            .expect("1m candle should persist");
    }

    let source = get_closed_1m_candles_range(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 5, 0).unwrap(),
    )
    .await
    .expect("1m range should load");
    let aggregated = aggregate_closed_1m_candles(&source, CandleInterval::FiveMinutes);
    upsert_aggregated_candles(&test_db.pool, &aggregated.candles)
        .await
        .expect("aggregated candles should persist");

    let one_minute_count = count_candles_by_interval(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
        CandleInterval::OneMinute,
    )
    .await
    .expect("1m count should work");
    let coverage = get_aggregated_candle_coverage(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
    )
    .await
    .expect("coverage should load");

    assert_eq!(one_minute_count, 5);
    assert_eq!(
        coverage
            .intervals
            .iter()
            .find(|entry| entry.interval == "1m")
            .map(|entry| entry.candle_count),
        Some(5)
    );
    assert_eq!(
        coverage
            .intervals
            .iter()
            .find(|entry| entry.interval == "5m")
            .map(|entry| entry.candle_count),
        Some(1)
    );
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

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn private_stream_event_persists_and_lists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    insert_exchange_private_stream_event(
        &test_db.pool,
        &ExchangePrivateStreamEventRecord {
            id: Uuid::new_v4(),
            exchange: "binance".to_string(),
            environment: "testnet".to_string(),
            event_type: "executionReport".to_string(),
            symbol: Some("BTCUSDT".to_string()),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("123".to_string()),
            execution_type: Some("TRADE".to_string()),
            order_status: Some("FILLED".to_string()),
            payload: json!({"e":"executionReport"}),
            event_time: fixed_time(),
            received_at: fixed_time(),
            correlation_id: None,
        },
    )
    .await
    .expect("private stream event should persist");

    let events = list_exchange_private_stream_events(
        &test_db.pool,
        "testnet",
        10,
        Some("client-1"),
        Some("executionReport"),
    )
    .await
    .expect("private stream events should list");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].client_order_id.as_deref(), Some("client-1"));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn private_stream_state_updates_and_testnet_order_mapping_stays_isolated() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record();

    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let state =
        upsert_exchange_private_stream_state(&test_db.pool, &sample_private_stream_state_record())
            .await
            .expect("private stream state should upsert");
    assert_eq!(state.status, "CONNECTED");

    let loaded = get_exchange_private_stream_state(&test_db.pool, "binance", "testnet")
        .await
        .expect("private stream state should load")
        .expect("private stream state should exist");
    assert_eq!(loaded.listen_key_hash.as_deref(), Some("hash123"));

    let updated = update_exchange_testnet_order_status(
        &test_db.pool,
        &order.client_order_id,
        order.exchange_order_id.as_deref(),
        "FILLED",
        "FILLED",
        &json!({"source":"private_stream"}),
        Some(fixed_time()),
    )
    .await
    .expect("testnet order should update")
    .expect("testnet order should exist");

    assert_eq!(updated.status, "FILLED");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn exchange_testnet_submit_lifecycle_events_persist_in_order() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record_with_state(
        "PREPARED",
        TestnetExecutionState::OrderPrepared,
    );
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let submit_event = lifecycle_event_record(
        &order,
        Some(TestnetExecutionState::OrderPrepared),
        TestnetExecutionState::OrderSubmitRequested,
        TestnetExecutionTransitionSource::ApiSubmit,
        Some("submit_requested"),
        Some(json!({"step":"submit"})),
        lifecycle_time(1),
    );
    let ack_payload = json!({"status":"NEW","source":"ack"});
    let ack_event = lifecycle_event_record(
        &order,
        Some(TestnetExecutionState::OrderSubmitRequested),
        TestnetExecutionState::ExchangeAcked,
        TestnetExecutionTransitionSource::ExchangeAck,
        Some("exchange_ack"),
        Some(ack_payload.clone()),
        lifecycle_time(2),
    );

    let _submitted = append_lifecycle_event(
        &test_db.pool,
        &submit_event,
        order.exchange_order_id.as_deref(),
        Some("SUBMIT_REQUESTED"),
        TestnetExecutionState::OrderSubmitRequested,
        Some(&json!({"status":"SUBMIT_REQUESTED"})),
        None,
    )
    .await;
    let acked = append_lifecycle_event(
        &test_db.pool,
        &ack_event,
        order.exchange_order_id.as_deref(),
        Some("NEW"),
        TestnetExecutionState::ExchangeAcked,
        None,
        Some(&ack_payload),
    )
    .await;

    let events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
            .await
            .expect("lifecycle events should list");

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].next_state, "ORDER_SUBMIT_REQUESTED");
    assert_eq!(events[0].previous_state.as_deref(), Some("ORDER_PREPARED"));
    assert_eq!(events[0].transition_source, "API_SUBMIT");
    assert_eq!(events[1].next_state, "EXCHANGE_ACKED");
    assert_eq!(
        events[1].previous_state.as_deref(),
        Some("ORDER_SUBMIT_REQUESTED")
    );
    assert_eq!(events[1].transition_source, "EXCHANGE_ACK");
    assert!(events[0].created_at < events[1].created_at);
    assert_eq!(acked.execution_state, "EXCHANGE_ACKED");
    assert_eq!(acked.ack_payload, Some(ack_payload));
    assert_eq!(acked.status, "NEW");
    assert_eq!(acked.last_transition_at, Some(lifecycle_time(2)));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn private_stream_transition_appends_lifecycle_event_and_updates_order_state() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record_with_state(
        "NEW",
        TestnetExecutionState::ExchangeAcked,
    );
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let report_payload = json!({
        "e":"executionReport",
        "X":"PARTIALLY_FILLED",
        "x":"TRADE",
        "c": order.client_order_id,
        "i": order.exchange_order_id,
        "s": order.symbol,
    });
    let report = sample_private_execution_report(
        &order,
        ExchangeExecutionStatus::PartiallyFilled,
        ExchangeExecutionReportType::Trade,
        report_payload.clone(),
    );
    insert_exchange_private_stream_event(
        &test_db.pool,
        &ExchangePrivateStreamEventRecord {
            id: Uuid::new_v4(),
            exchange: "binance".to_string(),
            environment: "testnet".to_string(),
            event_type: "executionReport".to_string(),
            symbol: Some(order.symbol.clone()),
            client_order_id: Some(order.client_order_id.clone()),
            exchange_order_id: order.exchange_order_id.clone(),
            execution_type: Some(report.execution_type.as_str().to_string()),
            order_status: Some(report.order_status.as_str().to_string()),
            payload: report_payload.clone(),
            event_time: report.event_time,
            received_at: report.event_time,
            correlation_id: None,
        },
    )
    .await
    .expect("private stream event should persist");

    let updated = apply_private_stream_report(&test_db.pool, &report).await;
    let events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
            .await
            .expect("lifecycle events should list");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].transition_source, "PRIVATE_STREAM");
    assert_eq!(events[0].next_state, "PARTIALLY_FILLED");
    assert_eq!(updated.execution_state, "PARTIALLY_FILLED");
    assert_eq!(updated.status, "PARTIALLY_FILLED");
    assert_eq!(updated.latest_status_payload, Some(report_payload));
    assert!(list_orders(&test_db.pool)
        .await
        .expect("orders should list")
        .is_empty());
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn rest_reconciliation_transition_appends_lifecycle_event_and_updates_order_state() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record_with_state("NEW", TestnetExecutionState::New);
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let rest_status = sample_exchange_order_status(
        &order,
        ExchangeOrderState::Filled,
        json!({"status":"FILLED","source":"rest_reconciliation"}),
    );
    let run = insert_exchange_reconciliation_run(
        &test_db.pool,
        &sample_exchange_reconciliation_run_record(),
    )
    .await
    .expect("reconciliation run should persist");

    let updated = apply_rest_reconciliation_status(&test_db.pool, &order, &rest_status).await;
    let mismatch = insert_exchange_reconciliation_mismatch(
        &test_db.pool,
        &ExchangeReconciliationMismatchRecord {
            id: Uuid::new_v4(),
            run_id: run.id,
            client_order_id: order.client_order_id.clone(),
            local_status: Some("NEW".to_string()),
            exchange_status: Some("FILLED".to_string()),
            mismatch_kind: ExchangeReconciliationMismatchKind::StatusMismatch
                .as_str()
                .to_string(),
            action: ExchangeReconciliationAction::UpdateLocalStatus
                .as_str()
                .to_string(),
            payload: json!({
                "reason": "local and exchange statuses differ",
                "local_status": "NEW",
                "exchange_status": "FILLED"
            }),
            created_at: lifecycle_time(21),
        },
    )
    .await
    .expect("reconciliation mismatch should persist");
    let events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
            .await
            .expect("lifecycle events should list");
    let mismatches = list_exchange_reconciliation_mismatches(&test_db.pool, run.id)
        .await
        .expect("reconciliation mismatches should list");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].transition_source, "REST_RECONCILIATION");
    assert_eq!(events[0].next_state, "FILLED");
    assert_eq!(updated.execution_state, "FILLED");
    assert_eq!(updated.status, "FILLED");
    assert_eq!(
        updated.latest_status_payload,
        Some(json!({"status":"FILLED","source":"rest_reconciliation"}))
    );
    assert_eq!(mismatches.len(), 1);
    assert_eq!(mismatches[0].id, mismatch.id);
    assert_eq!(mismatches[0].mismatch_kind, "STATUS_MISMATCH");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn invalid_rest_reconciliation_transition_becomes_reconciliation_required() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order =
        sample_exchange_testnet_order_record_with_state("FILLED", TestnetExecutionState::Filled);
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let rest_status = sample_exchange_order_status(
        &order,
        ExchangeOrderState::New,
        json!({"status":"NEW","source":"rest_reconciliation"}),
    );

    let updated = apply_rest_reconciliation_status(&test_db.pool, &order, &rest_status).await;
    let events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
            .await
            .expect("lifecycle events should list");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].transition_source, "REST_RECONCILIATION");
    assert_eq!(events[0].previous_state.as_deref(), Some("FILLED"));
    assert_eq!(events[0].next_state, "RECONCILIATION_REQUIRED");
    assert_eq!(
        events[0].reason.as_deref(),
        Some("invalid_rest_reconciliation_transition")
    );
    assert_eq!(updated.execution_state, "RECONCILIATION_REQUIRED");
    assert_eq!(updated.status, "NEW");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn cancel_lifecycle_path_persists_and_terminal_state_blocks_future_active_transition() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let order = sample_exchange_testnet_order_record_with_state("NEW", TestnetExecutionState::New);
    insert_exchange_testnet_order(&test_db.pool, &order)
        .await
        .expect("testnet order should persist");

    let cancel_requested = lifecycle_event_record(
        &order,
        Some(TestnetExecutionState::New),
        TestnetExecutionState::CancelRequested,
        TestnetExecutionTransitionSource::ApiCancel,
        Some("api_cancel"),
        Some(json!({"status":"PENDING_CANCEL"})),
        lifecycle_time(30),
    );
    let cancelled_payload = json!({"status":"CANCELLED","source":"cancel_ack"});
    let cancelled = lifecycle_event_record(
        &order,
        Some(TestnetExecutionState::CancelRequested),
        TestnetExecutionState::Cancelled,
        TestnetExecutionTransitionSource::ExchangeCancelAck,
        Some("exchange_cancel_ack"),
        Some(cancelled_payload.clone()),
        lifecycle_time(31),
    );

    let _cancel_requested = append_lifecycle_event(
        &test_db.pool,
        &cancel_requested,
        order.exchange_order_id.as_deref(),
        Some("PENDING_CANCEL"),
        TestnetExecutionState::CancelRequested,
        Some(&json!({"status":"PENDING_CANCEL"})),
        None,
    )
    .await;
    let cancelled_order = append_lifecycle_event(
        &test_db.pool,
        &cancelled,
        order.exchange_order_id.as_deref(),
        Some("CANCELLED"),
        TestnetExecutionState::Cancelled,
        Some(&cancelled_payload),
        None,
    )
    .await;
    let events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &order.client_order_id)
            .await
            .expect("lifecycle events should list");
    let invalid_transition = apply_testnet_transition(
        &aegis_core::TestnetOrderLifecycleSnapshot {
            order_id: Some(cancelled_order.id),
            client_order_id: cancelled_order.client_order_id.clone(),
            exchange_order_id: cancelled_order.exchange_order_id.clone(),
            current_state: TestnetExecutionState::Cancelled,
            last_transition_at: cancelled_order.last_transition_at,
        },
        TestnetExecutionState::New,
        TestnetExecutionTransitionSource::PrivateStream,
        Some("execution_report_new".to_string()),
        Some(json!({"status":"NEW"})),
    );

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].transition_source, "API_CANCEL");
    assert_eq!(events[1].transition_source, "EXCHANGE_CANCEL_ACK");
    assert_eq!(events[1].next_state, "CANCELLED");
    assert_eq!(cancelled_order.execution_state, "CANCELLED");
    assert!(invalid_transition.is_err());
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn lifecycle_event_listing_is_scoped_to_client_order_id_and_chronological() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let target_order = sample_exchange_testnet_order_record_with_state(
        "NEW",
        TestnetExecutionState::OrderSubmitRequested,
    );
    let other_order = sample_exchange_testnet_order_record_with_state(
        "NEW",
        TestnetExecutionState::OrderSubmitRequested,
    );
    insert_exchange_testnet_order(&test_db.pool, &target_order)
        .await
        .expect("target order should persist");
    insert_exchange_testnet_order(&test_db.pool, &other_order)
        .await
        .expect("other order should persist");

    let target_first = lifecycle_event_record(
        &target_order,
        Some(TestnetExecutionState::OrderSubmitRequested),
        TestnetExecutionState::ExchangeAcked,
        TestnetExecutionTransitionSource::ExchangeAck,
        Some("exchange_ack"),
        Some(json!({"status":"NEW"})),
        lifecycle_time(40),
    );
    let target_second = lifecycle_event_record(
        &target_order,
        Some(TestnetExecutionState::ExchangeAcked),
        TestnetExecutionState::PartiallyFilled,
        TestnetExecutionTransitionSource::PrivateStream,
        Some("execution_report_trade"),
        Some(json!({"status":"PARTIALLY_FILLED"})),
        lifecycle_time(41),
    );
    let other_event = lifecycle_event_record(
        &other_order,
        Some(TestnetExecutionState::OrderSubmitRequested),
        TestnetExecutionState::ExchangeAcked,
        TestnetExecutionTransitionSource::ExchangeAck,
        Some("exchange_ack"),
        Some(json!({"status":"NEW"})),
        lifecycle_time(42),
    );

    let _ = append_lifecycle_event(
        &test_db.pool,
        &target_first,
        target_order.exchange_order_id.as_deref(),
        Some("NEW"),
        TestnetExecutionState::ExchangeAcked,
        None,
        Some(&json!({"status":"NEW"})),
    )
    .await;
    let _ = append_lifecycle_event(
        &test_db.pool,
        &target_second,
        target_order.exchange_order_id.as_deref(),
        Some("PARTIALLY_FILLED"),
        TestnetExecutionState::PartiallyFilled,
        Some(&json!({"status":"PARTIALLY_FILLED"})),
        None,
    )
    .await;
    let _ = append_lifecycle_event(
        &test_db.pool,
        &other_event,
        other_order.exchange_order_id.as_deref(),
        Some("NEW"),
        TestnetExecutionState::ExchangeAcked,
        None,
        Some(&json!({"status":"NEW"})),
    )
    .await;

    let target_events =
        list_exchange_testnet_order_lifecycle_events(&test_db.pool, &target_order.client_order_id)
            .await
            .expect("target lifecycle events should list");

    assert_eq!(target_events.len(), 2);
    assert!(target_events
        .iter()
        .all(|event| event.client_order_id == target_order.client_order_id));
    assert_eq!(target_events[0].created_at, lifecycle_time(40));
    assert_eq!(target_events[1].created_at, lifecycle_time(41));
    assert_eq!(target_events[0].next_state, "EXCHANGE_ACKED");
    assert_eq!(target_events[1].next_state, "PARTIALLY_FILLED");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn testnet_shadow_runner_config_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let now = fixed_time();
    let record = upsert_testnet_shadow_runner_config(
        &test_db.pool,
        &TestnetShadowRunnerConfig {
            id: TESTNET_SHADOW_RUNNER_CONFIG_ID,
            enabled: true,
            interval_seconds: 60,
            strategies: vec!["momentum_v1".to_string()],
            symbols: vec!["BTCUSDT".to_string()],
            timeframe: "1m".to_string(),
            max_runs_per_tick: 2,
            stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy::Skip,
            notes: Some("db test".to_string()),
            updated_by: None,
            updated_at: now,
        },
    )
    .await
    .expect("runner config should persist");

    let mapped =
        testnet_shadow_runner_config_from_record(&record).expect("runner config should map");
    assert!(mapped.enabled);
    assert_eq!(mapped.max_runs_per_tick, 2);
    assert_eq!(mapped.symbols, vec!["BTCUSDT".to_string()]);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn testnet_shadow_runner_state_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let now = fixed_time();
    let record = upsert_testnet_shadow_runner_state(
        &test_db.pool,
        &aegis_core::TestnetShadowRunnerState {
            id: TESTNET_SHADOW_RUNNER_STATE_ID,
            status: TestnetShadowRunnerStatus::Paused,
            last_tick_at: Some(now),
            last_success_at: Some(now),
            last_error: Some("none".to_string()),
            total_ticks: 3,
            total_runs: 5,
            updated_at: now,
        },
    )
    .await
    .expect("runner state should persist");

    let mapped = testnet_shadow_runner_state_from_record(&record).expect("runner state should map");
    assert_eq!(mapped.status, TestnetShadowRunnerStatus::Paused);
    assert_eq!(mapped.total_ticks, 3);
    assert_eq!(mapped.total_runs, 5);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_reads_shadow_runs() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_testnet_shadow_run(
        &test_db.pool,
        &TestnetShadowRunRecord {
            id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            decision: "WOULD_SUBMIT".to_string(),
            signal_id: None,
            risk_decision_id: None,
            would_submit_payload: Some(json!({"symbol":"BTCUSDT"})),
            price_source: Some("stored_tick".to_string()),
            resolved_price: Some(Decimal::new(100_000, 0)),
            reasons: Vec::new(),
            status: "COMPLETED".to_string(),
            created_at: fixed_time(),
            correlation_id: Some(Uuid::new_v4()),
        },
    )
    .await
    .expect("shadow run should persist");

    let summary = get_strategy_performance_summary(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Shadow,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("summary should load");

    assert_eq!(summary.shadow_would_submit_count, 1);
    assert_eq!(summary.total_runs, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_returns_empty_combined_when_no_data() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    let summary = get_strategy_performance_summary(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: None,
            symbol: None,
            timeframe: None,
            mode: StrategyPerformanceMode::Combined,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("empty combined summary should load");

    assert_eq!(summary.mode, StrategyPerformanceMode::Combined);
    assert_eq!(summary.total_runs, 0);
    assert_eq!(summary.total_signals, 0);
    assert_eq!(summary.approved_risk_decisions, 0);
    assert_eq!(summary.rejected_risk_decisions, 0);
    assert_eq!(summary.paper_positions_opened, 0);
    assert_eq!(summary.paper_positions_closed, 0);
    assert_eq!(summary.backtest_runs_count, 0);
    assert_eq!(summary.shadow_would_submit_count, 0);
    assert_eq!(summary.shadow_no_signal_count, 0);
    assert_eq!(summary.shadow_risk_rejected_count, 0);
    assert_eq!(summary.realized_pnl, Decimal::ZERO);
    assert_eq!(summary.unrealized_pnl, Decimal::ZERO);
    assert_eq!(summary.risk_rejection_rate, Decimal::ZERO);
    assert_eq!(summary.win_rate, None);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_returns_empty_filtered_combined_when_no_matching_data() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    let summary = get_strategy_performance_summary(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Combined,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("empty filtered combined summary should load");

    assert_eq!(summary.strategy_id.as_deref(), Some("momentum_v1"));
    assert_eq!(summary.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(summary.timeframe.as_deref(), Some("1m"));
    assert_eq!(summary.total_runs, 0);
    assert_eq!(summary.total_signals, 0);
    assert_eq!(summary.realized_pnl, Decimal::ZERO);
    assert_eq!(summary.risk_rejection_rate, Decimal::ZERO);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_combined_tolerates_missing_modes() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let request = sample_backtest_request();
    let config = sample_backtest_config();
    let run_id = Uuid::new_v4();
    insert_backtest_run(
        &test_db.pool,
        run_id,
        &request,
        &config,
        fixed_time(),
        ReplayRunStatus::Pending,
        request.correlation_id,
    )
    .await
    .expect("backtest run should persist");
    update_backtest_run_completed(
        &test_db.pool,
        &BacktestResult {
            run_id,
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            initial_capital: request.initial_capital,
            final_equity: Decimal::new(1_050_000, 0),
            pnl: Decimal::new(50_000, 0),
            pnl_pct: Decimal::new(5, 0),
            max_drawdown_pct: Decimal::new(1, 0),
            win_rate: Decimal::new(5, 1),
            trade_count: 2,
            winning_trades: 1,
            losing_trades: 1,
            avg_win: Decimal::new(10_000, 0),
            avg_loss: Decimal::new(-5_000, 0),
            fee_paid: Decimal::new(100, 0),
            slippage_cost: Decimal::new(50, 0),
            status: ReplayRunStatus::Completed,
            created_at: fixed_time(),
            correlation_id: request.correlation_id,
        },
        &config,
    )
    .await
    .expect("backtest completion should persist");

    let summary = get_strategy_performance_summary(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Combined,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("combined summary should load with only backtest data");

    assert_eq!(summary.mode, StrategyPerformanceMode::Combined);
    assert_eq!(summary.total_runs, 1);
    assert_eq!(summary.backtest_runs_count, 1);
    assert_eq!(summary.paper_positions_opened, 0);
    assert_eq!(summary.paper_positions_closed, 0);
    assert_eq!(summary.shadow_would_submit_count, 0);
    assert_eq!(summary.realized_pnl, Decimal::new(50_000, 0));
    assert_eq!(summary.best_backtest_pnl_pct, Some(Decimal::new(5, 0)));
    assert_eq!(summary.avg_backtest_pnl_pct, Some(Decimal::new(5, 0)));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_reads_backtest_runs() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let request = sample_backtest_request();
    let config = sample_backtest_config();
    let run_id = Uuid::new_v4();
    insert_backtest_run(
        &test_db.pool,
        run_id,
        &request,
        &config,
        fixed_time(),
        ReplayRunStatus::Pending,
        request.correlation_id,
    )
    .await
    .expect("backtest run should persist");
    update_backtest_run_completed(
        &test_db.pool,
        &BacktestResult {
            run_id,
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            initial_capital: request.initial_capital,
            final_equity: Decimal::new(1_050_000, 0),
            pnl: Decimal::new(50_000, 0),
            pnl_pct: Decimal::new(5, 0),
            max_drawdown_pct: Decimal::new(1, 0),
            win_rate: Decimal::new(5, 1),
            trade_count: 2,
            winning_trades: 1,
            losing_trades: 1,
            avg_win: Decimal::new(10_000, 0),
            avg_loss: Decimal::new(-5_000, 0),
            fee_paid: Decimal::new(100, 0),
            slippage_cost: Decimal::new(50, 0),
            status: ReplayRunStatus::Completed,
            created_at: fixed_time(),
            correlation_id: request.correlation_id,
        },
        &config,
    )
    .await
    .expect("backtest completion should persist");

    let summary = get_strategy_performance_summary(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Backtest,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("summary should load");

    assert_eq!(summary.backtest_runs_count, 1);
    assert_eq!(summary.best_backtest_pnl_pct, Some(Decimal::new(5, 0)));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_performance_summary_reads_paper_positions() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let account = sample_paper_account();
    insert_paper_account(&test_db.pool, &account)
        .await
        .expect("paper account should persist");
    upsert_paper_position(&test_db.pool, &sample_paper_position(account.id))
        .await
        .expect("paper position should persist");

    let breakdown = get_strategy_paper_pnl_breakdown(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: None,
            mode: StrategyPerformanceMode::Paper,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("paper pnl breakdown should load");

    assert_eq!(breakdown.positions_closed, 1);
    assert_eq!(breakdown.realized_pnl, Decimal::new(5_000, 0));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_rankings_orders_strategies_correctly() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    for (strategy_id, pnl) in [("momentum_v1", 5), ("volatility_breakout_v1", 2)] {
        let mut request = sample_backtest_request();
        request.strategy_id = strategy_id.to_string();
        let config = sample_backtest_config();
        let run_id = Uuid::new_v4();
        insert_backtest_run(
            &test_db.pool,
            run_id,
            &request,
            &config,
            fixed_time(),
            ReplayRunStatus::Pending,
            request.correlation_id,
        )
        .await
        .expect("backtest run should persist");
        update_backtest_run_completed(
            &test_db.pool,
            &BacktestResult {
                run_id,
                strategy_id: request.strategy_id.clone(),
                symbol: request.symbol.clone(),
                timeframe: request.timeframe.clone(),
                start_time: request.start_time,
                end_time: request.end_time,
                initial_capital: request.initial_capital,
                final_equity: Decimal::new(1_000_000 + pnl * 10_000, 0),
                pnl: Decimal::new(pnl * 10_000, 0),
                pnl_pct: Decimal::new(pnl, 0),
                max_drawdown_pct: Decimal::ONE,
                win_rate: Decimal::new(5, 1),
                trade_count: 1,
                winning_trades: 1,
                losing_trades: 0,
                avg_win: Decimal::new(pnl * 10_000, 0),
                avg_loss: Decimal::ZERO,
                fee_paid: Decimal::ZERO,
                slippage_cost: Decimal::ZERO,
                status: ReplayRunStatus::Completed,
                created_at: fixed_time(),
                correlation_id: request.correlation_id,
            },
            &config,
        )
        .await
        .expect("backtest completion should persist");
    }

    let rankings = list_strategy_performance_rankings(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: None,
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Backtest,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: Some(20),
        },
    )
    .await
    .expect("rankings should load");

    assert_eq!(
        rankings.first().map(|item| item.strategy_id.as_str()),
        Some("momentum_v1")
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_experiment_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let experiment_id = Uuid::new_v4();
    let runs = vec![
        sample_strategy_experiment_run(experiment_id, 1, 8),
        sample_strategy_experiment_run(experiment_id, 2, 4),
    ];
    let experiment = sample_strategy_experiment_result(experiment_id, &runs);

    insert_strategy_experiment(&test_db.pool, &experiment)
        .await
        .expect("experiment should persist");

    let listed = list_strategy_experiments(&test_db.pool, 10)
        .await
        .expect("experiments should list");

    assert!(listed.iter().any(|record| record.id == experiment_id));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_experiment_runs_persist() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let experiment_id = Uuid::new_v4();
    let runs = vec![
        sample_strategy_experiment_run(experiment_id, 1, 8),
        sample_strategy_experiment_run(experiment_id, 2, 4),
    ];
    let experiment = sample_strategy_experiment_result(experiment_id, &runs);

    insert_strategy_experiment(&test_db.pool, &experiment)
        .await
        .expect("experiment should persist");
    insert_strategy_experiment_runs(&test_db.pool, &runs)
        .await
        .expect("experiment runs should persist");

    let persisted = list_strategy_experiment_runs(&test_db.pool, experiment_id)
        .await
        .expect("experiment runs should list");

    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].rank, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_experiment_read_model_returns_ranked_results_without_execution_mutation() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let experiment_id = Uuid::new_v4();
    let runs = vec![
        sample_strategy_experiment_run(experiment_id, 1, 8),
        sample_strategy_experiment_run(experiment_id, 2, 4),
    ];
    let experiment = sample_strategy_experiment_result(experiment_id, &runs);

    insert_strategy_experiment(&test_db.pool, &experiment)
        .await
        .expect("experiment should persist");
    insert_strategy_experiment_runs(&test_db.pool, &runs)
        .await
        .expect("experiment runs should persist");

    let listed = list_strategy_experiment_runs(&test_db.pool, experiment_id)
        .await
        .expect("experiment runs should list");
    let mapped = strategy_experiment_result_from_records(
        &list_strategy_experiments(&test_db.pool, 1)
            .await
            .expect("experiments should list")[0],
        &listed,
    )
    .expect("strategy experiment read model should map");

    assert_eq!(mapped.best_run.as_ref().map(|run| run.rank), Some(1));
    assert_eq!(
        list_orders(&test_db.pool)
            .await
            .expect("orders should load")
            .len(),
        0
    );
    assert_eq!(
        get_strategy_shadow_decision_breakdown(
            &test_db.pool,
            &StrategyPerformanceRequest {
                strategy_id: Some("momentum_v1".to_string()),
                symbol: Some("BTCUSDT".to_string()),
                timeframe: Some("1m".to_string()),
                mode: StrategyPerformanceMode::Shadow,
                start_time: Some(fixed_time() - chrono::Duration::days(1)),
                end_time: Some(fixed_time() + chrono::Duration::days(1)),
                limit: None,
            },
        )
        .await
        .expect("shadow breakdown should load")
        .total_runs,
        0
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_walk_forward_run_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let walk_forward_id = Uuid::new_v4();
    let request = sample_strategy_walk_forward_request();
    let result = sample_strategy_walk_forward_result(walk_forward_id);

    insert_strategy_walk_forward_run(&test_db.pool, &request, &result)
        .await
        .expect("walk-forward should persist");

    let listed = list_strategy_walk_forward_runs(&test_db.pool, 10)
        .await
        .expect("walk-forward runs should list");

    assert!(listed.iter().any(|record| record.id == walk_forward_id));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_walk_forward_windows_persist_and_order() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let walk_forward_id = Uuid::new_v4();
    let request = sample_strategy_walk_forward_request();
    let result = sample_strategy_walk_forward_result(walk_forward_id);
    let windows = sample_strategy_walk_forward_windows(walk_forward_id);

    insert_strategy_walk_forward_run(&test_db.pool, &request, &result)
        .await
        .expect("walk-forward should persist");
    insert_strategy_walk_forward_windows(&test_db.pool, &windows)
        .await
        .expect("walk-forward windows should persist");

    let persisted = list_strategy_walk_forward_windows(&test_db.pool, walk_forward_id)
        .await
        .expect("walk-forward windows should list");

    assert_eq!(persisted.len(), 3);
    assert_eq!(persisted[0].window_index, 0);
    assert_eq!(persisted[1].window_index, 1);
    assert_eq!(persisted[2].window_index, 2);
    assert_eq!(
        persisted[2].skip_reason.as_deref(),
        Some("insufficient_candle_coverage: expected=96 actual=80 required=10")
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_walk_forward_read_model_maps_ordered_windows_without_execution_mutation() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let walk_forward_id = Uuid::new_v4();
    let request = sample_strategy_walk_forward_request();
    let result = sample_strategy_walk_forward_result(walk_forward_id);
    let windows = sample_strategy_walk_forward_windows(walk_forward_id);

    insert_strategy_walk_forward_run(&test_db.pool, &request, &result)
        .await
        .expect("walk-forward should persist");
    insert_strategy_walk_forward_windows(&test_db.pool, &windows)
        .await
        .expect("walk-forward windows should persist");

    let run_record = list_strategy_walk_forward_runs(&test_db.pool, 1)
        .await
        .expect("walk-forward runs should list")
        .remove(0);
    let window_records = list_strategy_walk_forward_windows(&test_db.pool, walk_forward_id)
        .await
        .expect("walk-forward windows should list");
    let mapped = strategy_walk_forward_result_from_records(&run_record, &window_records)
        .expect("walk-forward read model should map");
    let mapped_windows = window_records
        .iter()
        .map(strategy_walk_forward_window_from_record)
        .collect::<Result<Vec<_>, _>>()
        .expect("window records should map");

    assert_eq!(mapped.walk_forward_id, walk_forward_id);
    assert_eq!(mapped_windows[0].window.window_index, 0);
    assert_eq!(
        list_orders(&test_db.pool)
            .await
            .expect("orders should load")
            .len(),
        0
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn strategy_decision_breakdown_counts_shadow_outcomes() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    for decision in ["WOULD_SUBMIT", "NO_SIGNAL", "RISK_REJECTED"] {
        insert_testnet_shadow_run(
            &test_db.pool,
            &TestnetShadowRunRecord {
                id: Uuid::new_v4(),
                strategy_id: "momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "1m".to_string(),
                decision: decision.to_string(),
                signal_id: None,
                risk_decision_id: None,
                would_submit_payload: None,
                price_source: None,
                resolved_price: None,
                reasons: Vec::new(),
                status: "COMPLETED".to_string(),
                created_at: fixed_time(),
                correlation_id: Some(Uuid::new_v4()),
            },
        )
        .await
        .expect("shadow run should persist");
    }

    let breakdown = get_strategy_shadow_decision_breakdown(
        &test_db.pool,
        &StrategyPerformanceRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Shadow,
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("decision breakdown should load");

    assert_eq!(breakdown.would_submit_count, 1);
    assert_eq!(breakdown.no_signal_count, 1);
    assert_eq!(breakdown.risk_rejected_count, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promotion_funnel_counts_shadow_would_submit_rows() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_testnet_shadow_run(
        &test_db.pool,
        &TestnetShadowRunRecord {
            id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            decision: "WOULD_SUBMIT".to_string(),
            signal_id: None,
            risk_decision_id: None,
            would_submit_payload: Some(json!({"symbol":"BTCUSDT"})),
            price_source: None,
            resolved_price: None,
            reasons: Vec::new(),
            status: "COMPLETED".to_string(),
            created_at: fixed_time(),
            correlation_id: Some(Uuid::new_v4()),
        },
    )
    .await
    .expect("shadow run should persist");

    let summary = get_testnet_promotion_funnel_summary(
        &test_db.pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("summary should load");

    assert_eq!(summary.shadow_would_submit_count, 1);
    assert_eq!(summary.promotion_previewed_count, 0);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promotion_funnel_counts_previewed_and_submitted_promotions() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_promotion_funnel_fixture(&test_db.pool, "BTCUSDT", "PREVIEWED", None, false).await;
    insert_promotion_funnel_fixture(
        &test_db.pool,
        "ETHUSDT",
        "SUBMITTED",
        Some(TestnetExecutionState::Filled),
        false,
    )
    .await;

    let summary = get_testnet_promotion_funnel_summary(
        &test_db.pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: None,
            timeframe: Some("1m".to_string()),
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("summary should load");

    assert_eq!(summary.promotion_previewed_count, 2);
    assert_eq!(summary.promotion_submitted_count, 1);
    assert_eq!(summary.filled_count, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promotion_funnel_counts_linked_order_lifecycle_state() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_promotion_funnel_fixture(
        &test_db.pool,
        "BTCUSDT",
        "SUBMITTED",
        Some(TestnetExecutionState::ReconciliationRequired),
        false,
    )
    .await;

    let lifecycle = get_testnet_promotion_lifecycle_breakdown(
        &test_db.pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("lifecycle breakdown should load");

    assert_eq!(
        lifecycle
            .iter()
            .find(|item| item.execution_state
                == TestnetExecutionState::ReconciliationRequired.as_str())
            .map(|item| item.count),
        Some(1),
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promotion_rows_do_not_leak_unrelated_symbols() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_promotion_funnel_fixture(
        &test_db.pool,
        "BTCUSDT",
        "SUBMITTED",
        Some(TestnetExecutionState::Filled),
        false,
    )
    .await;
    insert_promotion_funnel_fixture(
        &test_db.pool,
        "ETHUSDT",
        "SUBMITTED",
        Some(TestnetExecutionState::Cancelled),
        false,
    )
    .await;

    let rows = list_testnet_promotion_funnel_rows(
        &test_db.pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: Some(50),
        },
    )
    .await
    .expect("rows should load");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol, "BTCUSDT");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promotion_summary_handles_missing_linked_order_without_crashing() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_promotion_funnel_fixture(
        &test_db.pool,
        "BTCUSDT",
        "SUBMITTED",
        Some(TestnetExecutionState::Filled),
        true,
    )
    .await;

    let summary = get_testnet_promotion_funnel_summary(
        &test_db.pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            start_time: Some(fixed_time() - chrono::Duration::days(1)),
            end_time: Some(fixed_time() + chrono::Duration::days(1)),
            limit: None,
        },
    )
    .await
    .expect("summary should load");

    assert_eq!(summary.promotion_submitted_count, 1);
    assert_eq!(summary.testnet_orders_created_count, 1);
}
