use aegis_core::{
    aggregate_closed_1m_candles, BacktestConfig, BacktestEquityPoint, BacktestRequest,
    BacktestResult, BacktestTrade, Candle, CandleBackfillRequest, CandleBackfillStatus,
    CandleInterval, ExchangeEnvironment, ExchangeExecutionReport, ExchangeExecutionReportType,
    ExchangeExecutionStatus, ExchangeName, ExchangeOrderSide, ExchangeOrderState,
    ExchangeOrderStatus, ExchangeOrderTimeInForce, ExchangeOrderType, ExchangeReconciliationAction,
    ExchangeReconciliationMismatchKind, ExchangeReconciliationSummary, ExecutionReadinessStatus,
    FeeModel, MarketDataQualityRequest, MarketDataQualityStatus, MarketDataRepairPlan,
    MarketDataRepairRunResult, MarketDataRepairStatus, MarketDataSource, OrderIntent, PaperAccount,
    PaperAccountStatus, PaperPosition, PaperPriceStatus, PositionSide, PositionStatus, ReplayMode,
    ReplayRunStatus, ResearchCandidate, ResearchCandidateDecision, ResearchCandidateLifecycleEvent,
    ResearchCandidateStatus, ResearchDataCoverageResult, ResearchDataReadinessStatus,
    ResearchDatasetBuildRequest, ResearchDatasetBuildStatus, ResearchDatasetBuildStep,
    ResearchDatasetBuildStepStatus, ResearchHypothesis, ResearchHypothesisEvidence,
    ResearchHypothesisPriority, ResearchHypothesisRecommendation, ResearchHypothesisSource,
    ResearchHypothesisStatus, ResearchRegimeCalibrationCandidateResult,
    ResearchRegimeCalibrationRecommendation, ResearchRegimeCalibrationRequest,
    ResearchRegimeCalibrationResult, ResearchRegimeCalibrationStatus,
    ResearchRegimeClassificationExplanation, ResearchRegimeClassifierConfig,
    ResearchRegimeDiscoveryCandidateWindow, ResearchRegimeDiscoveryRecommendation,
    ResearchRegimeDiscoveryRequest, ResearchRegimeDiscoveryResult, ResearchRegimeDiscoveryStatus,
    ResearchRegimeDiscoverySummary, ResearchRegimeLabel, ResearchShadowPnlAttributionRequest,
    ResearchShadowPnlStatus, ResearchStaleRunRecoveryRequest, ResearchStaleRunRecoveryTargetType,
    RiskCheckContext, RiskEvaluationDecision, RiskEvaluationResult, RiskRuleDecision,
    RiskRuleResult, ScheduledResearchJobKind, ScheduledResearchJobRequest, ScheduledResearchJobRun,
    ScheduledResearchJobRunStatus, ScheduledResearchJobStatus, Side, SignalConfidence,
    SignalReason, SignalSide, StrategyCandidateObservationDecision,
    StrategyCandidateObservationFinding, StrategyCandidateObservationRequirement,
    StrategyCandidateObservationResult, StrategyCandidateObservationStatus,
    StrategyCandidateObservationSummary, StrategyCandidateRunnerAlignment, StrategyConfig,
    StrategyExperimentCandidate, StrategyExperimentComparison, StrategyExperimentMetric,
    StrategyExperimentResult, StrategyExperimentRun, StrategyExperimentStatus, StrategyId,
    StrategyMode, StrategyPerformanceMode, StrategyPerformanceRequest, StrategyResearchCandidate,
    StrategyResearchCandidateEvidence, StrategyResearchCandidateScore,
    StrategyResearchCandidateSource, StrategyResearchCandidateStatus, StrategyRobustnessMatrixCell,
    StrategyRobustnessMatrixFinding, StrategyRobustnessMatrixRecommendation,
    StrategyRobustnessMatrixRequest, StrategyRobustnessMatrixResult,
    StrategyRobustnessMatrixStatus, StrategyRobustnessMatrixStrategySummary,
    StrategyRobustnessMatrixWindow, StrategySignal, StrategyWalkForwardCandidate,
    StrategyWalkForwardRequest, StrategyWalkForwardResult, StrategyWalkForwardRobustnessSummary,
    StrategyWalkForwardStatus, StrategyWalkForwardWindow, StrategyWalkForwardWindowResult, Symbol,
    TestnetExecutionState, TestnetExecutionTransitionSource, TestnetPromotionFunnelRequest,
    TestnetShadowRunnerConfig, TestnetShadowRunnerStaleFeedPolicy, TestnetShadowRunnerStatus,
};
use chrono::{TimeZone, Utc};
use db::{
    append_exchange_testnet_lifecycle_event_and_update_order, append_research_candidate_event,
    complete_market_data_repair_run, count_candles_by_interval, count_candles_range,
    create_paper_order, create_research_candidate, decide_research_hypothesis,
    fail_exchange_reconciliation_run, get_aggregated_candle_coverage, get_backtest_equity_curve,
    get_backtest_run, get_backtest_trades, get_candle_backfill_run, get_candles_for_quality_report,
    get_closed_1m_candles_range, get_closed_candles_range, get_exchange_private_stream_state,
    get_exchange_reconciliation_run, get_exchange_testnet_order_by_client_order_id,
    get_order_by_idempotency_key, get_research_candidate_shadow_performance,
    get_research_candidate_shadow_pnl_attribution, get_research_dataset_build,
    get_research_hypothesis, get_research_regime_calibration, get_research_regime_discovery,
    get_risk_decision, get_scheduled_research_job_by_name, get_strategy_paper_pnl_breakdown,
    get_strategy_performance_summary, get_strategy_robustness_matrix_run,
    get_strategy_shadow_decision_breakdown, get_system_state, get_testnet_promotion_funnel_summary,
    get_testnet_promotion_lifecycle_breakdown, get_testnet_shadow_run_by_id,
    insert_backtest_equity_points, insert_backtest_run, insert_backtest_trade,
    insert_candle_backfill_run, insert_exchange_private_stream_event,
    insert_exchange_reconciliation_mismatch, insert_exchange_reconciliation_run,
    insert_exchange_testnet_order, insert_exchange_testnet_order_lifecycle_event,
    insert_market_data_repair_run, insert_paper_account, insert_research_candidate_shadow_run_link,
    insert_research_dataset_build, insert_research_hypothesis, insert_research_regime_calibration,
    insert_research_regime_discovery, insert_risk_decision, insert_scheduled_research_job,
    insert_scheduled_research_job_run, insert_signal_deduped,
    insert_strategy_candidate_observation, insert_strategy_experiment,
    insert_strategy_experiment_runs, insert_strategy_research_candidate,
    insert_strategy_robustness_matrix_cells, insert_strategy_robustness_matrix_run,
    insert_strategy_walk_forward_run, insert_strategy_walk_forward_windows,
    insert_testnet_shadow_promotion, insert_testnet_shadow_run,
    list_closed_candle_open_times_in_range, list_exchange_private_stream_events,
    list_exchange_reconciliation_mismatches, list_exchange_testnet_order_lifecycle_events,
    list_orders, list_recent_signals, list_research_candidate_shadow_runs,
    list_research_dataset_build_steps, list_research_hypotheses,
    list_research_regime_calibration_candidates, list_research_regime_calibrations,
    list_research_regime_discovery_windows, list_scheduled_research_job_runs,
    list_scheduled_research_jobs, list_strategy_candidate_observations,
    list_strategy_experiment_runs, list_strategy_experiments, list_strategy_performance_rankings,
    list_strategy_research_candidates, list_strategy_robustness_matrix_cells,
    list_strategy_robustness_matrix_runs, list_strategy_walk_forward_runs,
    list_strategy_walk_forward_windows, list_testnet_promotion_funnel_rows,
    list_testnet_shadow_runs, list_testnet_shadow_runs_in_window,
    mark_strategy_research_candidate_promoted, market_data_repair_result_from_record,
    recover_stale_research_runs_at, replace_research_dataset_build_steps,
    research_candidate_event_from_record, research_candidate_from_record,
    research_dataset_build_result_from_records, research_regime_calibration_result_from_records,
    research_regime_discovery_result_from_records,
    resolve_promoted_research_candidate_for_shadow_run, scheduled_research_job_from_record,
    scheduled_research_job_run_from_record, set_kill_switch_state,
    strategy_candidate_observation_result_from_record, strategy_experiment_result_from_records,
    strategy_research_candidate_from_record, strategy_robustness_matrix_cell_from_record,
    strategy_robustness_matrix_result_from_record, strategy_walk_forward_result_from_records,
    strategy_walk_forward_window_from_record, summarize_candle_continuity_report,
    test_support::TestDatabase, testnet_shadow_runner_config_from_record,
    testnet_shadow_runner_state_from_record, try_claim_scheduled_research_job,
    update_backtest_run_completed, update_exchange_testnet_order_status,
    update_scheduled_research_job_status, upsert_aggregated_candles, upsert_candle,
    upsert_candles_batch, upsert_exchange_private_stream_state, upsert_paper_position,
    upsert_testnet_shadow_runner_config, upsert_testnet_shadow_runner_state, CreateOrderError,
    ExchangePrivateStreamEventRecord, ExchangePrivateStreamStateRecord,
    ExchangeReconciliationMismatchRecord, ExchangeReconciliationRunRecord,
    ExchangeTestnetOrderLifecycleEventRecord, ExchangeTestnetOrderRecord,
    ResearchCandidateShadowPerformanceWindow, ResearchCandidateShadowRunsQuery,
    ShadowRunCandidateMatchOutcome, StateActor, StrategyResearchCandidateListFilters,
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

async fn execution_table_counts(pool: &sqlx::PgPool) -> Vec<(String, i64)> {
    sqlx::query_as(
        r#"
        SELECT name, count FROM (
            SELECT 'orders' AS name, COUNT(*)::BIGINT AS count FROM orders
            UNION ALL SELECT 'paper_positions', COUNT(*)::BIGINT FROM paper_positions
            UNION ALL SELECT 'paper_fills', COUNT(*)::BIGINT FROM paper_fills
            UNION ALL SELECT 'exchange_testnet_orders', COUNT(*)::BIGINT FROM exchange_testnet_orders
            UNION ALL SELECT 'exchange_testnet_order_lifecycle_events', COUNT(*)::BIGINT FROM exchange_testnet_order_lifecycle_events
            UNION ALL SELECT 'testnet_shadow_promotions', COUNT(*)::BIGINT FROM testnet_shadow_promotions
        ) counts
        ORDER BY name
        "#,
    )
    .fetch_all(pool)
    .await
    .expect("execution table counts should query")
}

fn sample_research_coverage_result() -> ResearchDataCoverageResult {
    ResearchDataCoverageResult {
        exchange: MarketDataSource::Binance,
        symbol: "BTCUSDT".to_string(),
        window_start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        window_end: Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        required_coverage_pct: Decimal::new(95, 0),
        status: ResearchDataReadinessStatus::Ready,
        per_interval: Vec::new(),
        correlation_id: Some(Uuid::from_u128(0x777)),
    }
}

#[tokio::test]
#[ignore = "requires Postgres test database"]
async fn scheduled_research_job_and_run_persist_without_execution_mutation() {
    let db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let before = execution_table_counts(&db.pool).await;
    let request = ScheduledResearchJobRequest {
        name: "Aggregation status".to_string(),
        kind: ScheduledResearchJobKind::AggregationStatus,
        enabled: false,
        interval_seconds: 60,
        request: json!({}),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let job_record = insert_scheduled_research_job(&db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");
    assert_eq!(job.kind, ScheduledResearchJobKind::AggregationStatus);
    assert!(!job.enabled);

    let run = ScheduledResearchJobRun {
        id: Uuid::new_v4(),
        job_id: job.id,
        status: ScheduledResearchJobRunStatus::Completed,
        started_at: fixed_time(),
        completed_at: Some(fixed_time()),
        result: json!({"ok": true}),
        error: None,
        created_artifact_type: Some("aggregation_status".to_string()),
        created_artifact_id: None,
        correlation_id: Some(Uuid::new_v4()),
    };
    let run_record = insert_scheduled_research_job_run(&db.pool, &run)
        .await
        .expect("scheduled run should persist");
    let mapped_run =
        scheduled_research_job_run_from_record(&run_record).expect("scheduled run should map");
    assert_eq!(mapped_run.status, ScheduledResearchJobRunStatus::Completed);

    let jobs = list_scheduled_research_jobs(&db.pool, 20)
        .await
        .expect("jobs should list");
    let runs = list_scheduled_research_job_runs(&db.pool, job.id, 20)
        .await
        .expect("runs should list");
    assert_eq!(jobs.len(), 1);
    assert_eq!(runs.len(), 1);
    assert_eq!(before, execution_table_counts(&db.pool).await);
}

#[tokio::test]
#[ignore = "requires Postgres test database"]
async fn scheduled_research_job_claim_prevents_double_run() {
    let db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let request = ScheduledResearchJobRequest {
        name: "Provider health".to_string(),
        kind: ScheduledResearchJobKind::ProviderHealth,
        enabled: true,
        interval_seconds: 60,
        request: json!({}),
        max_runs_per_tick: 1,
        next_run_at: Some(fixed_time()),
    };
    let job_record = insert_scheduled_research_job(&db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");
    let before = execution_table_counts(&db.pool).await;

    let first = try_claim_scheduled_research_job(&db.pool, job.id, fixed_time(), false)
        .await
        .expect("first claim should query");
    let second = try_claim_scheduled_research_job(&db.pool, job.id, fixed_time(), false)
        .await
        .expect("second claim should query");

    assert!(first.is_some());
    assert!(second.is_none());
    assert_eq!(before, execution_table_counts(&db.pool).await);
}

#[tokio::test]
#[ignore = "requires Postgres test database"]
async fn scheduled_research_bootstrap_name_lookup_prevents_duplicates_and_enables_existing() {
    let db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let before = execution_table_counts(&db.pool).await;
    let request = ScheduledResearchJobRequest {
        name: "provider-health-binance".to_string(),
        kind: ScheduledResearchJobKind::ProviderHealth,
        enabled: false,
        interval_seconds: 900,
        request: json!({"exchange": "binance"}),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let first = insert_scheduled_research_job(&db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let existing = get_scheduled_research_job_by_name(&db.pool, "provider-health-binance")
        .await
        .expect("name lookup should query")
        .expect("job should exist");
    assert_eq!(existing.id, first.id);

    let enabled = update_scheduled_research_job_status(
        &db.pool,
        existing.id,
        true,
        ScheduledResearchJobStatus::Enabled,
        Some(fixed_time()),
    )
    .await
    .expect("enable update should query")
    .expect("job should update");
    assert!(enabled.enabled);

    let jobs = list_scheduled_research_jobs(&db.pool, 20)
        .await
        .expect("jobs should list");
    assert_eq!(
        jobs.iter()
            .filter(|job| job.name == "provider-health-binance")
            .count(),
        1
    );
    assert_eq!(before, execution_table_counts(&db.pool).await);
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
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_breakdown_pct: None,
            min_reclaim_close_pct: None,
            min_lower_wick_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
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
        raw_signal_count: 5 * rank,
        cooldown_suppressed_count: 0,
        open_position_suppressed_count: 0,
        executed_trade_count: 5 * rank,
        suppression_breakdown: Vec::new(),
        last_signal_time: Some(fixed_time()),
        last_executed_entry_time: Some(fixed_time()),
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
        total_candidate_configs: runs.len() as i32,
        skipped_invalid_config_count: 0,
        executed_config_count: runs.len() as i32,
        invalid_config_examples: Vec::new(),
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

fn sample_strategy_config_for_candidate(
    strategy_id: StrategyId,
    symbol: &str,
    timeframe: CandleInterval,
    mode: StrategyMode,
    lookback_candles: u32,
) -> StrategyConfig {
    StrategyConfig {
        strategy_id,
        enabled: true,
        mode,
        symbols: vec![Symbol::new(symbol).expect("valid symbol")],
        timeframe,
        suggested_notional: Decimal::new(100_000, 0),
        max_signal_age_ms: 180_000,
        cooldown_seconds: 900,
        lookback_candles,
        trend_lookback_candles: None,
        momentum_lookback_candles: None,
        compression_lookback_candles: None,
        breakout_lookback_candles: None,
        pullback_lookback_candles: None,
        pullback_sma_lookback_candles: None,
        compression_percentile_threshold: None,
        min_breakout_pct: None,
        max_breakout_extension_pct: None,
        min_volume_expansion_ratio: None,
        lower_band_pct: None,
        upper_band_pct: None,
        min_range_width_pct: None,
        max_range_width_pct: None,
        min_close_above_sma_pct: None,
        max_close_above_sma_pct: None,
        min_momentum_return_pct: None,
        min_trend_return_pct: None,
        min_trend_slope_pct: None,
        min_pullback_depth_pct: None,
        max_pullback_depth_pct: None,
        min_reclaim_pct: None,
        min_breakdown_pct: None,
        min_reclaim_close_pct: None,
        min_lower_wick_pct: None,
        min_volume_ratio: None,
        max_choppiness: None,
        confidence_floor: None,
        stop_loss_pct: None,
        take_profit_pct: None,
        holding_candles: Some(3),
        notes: Some("research candidate fixture".to_string()),
    }
}

fn sample_research_candidate(
    id: Uuid,
    strategy_id: StrategyId,
    symbol: &str,
    timeframe: CandleInterval,
    source_type: StrategyResearchCandidateSource,
    status: StrategyResearchCandidateStatus,
    created_at: chrono::DateTime<Utc>,
) -> StrategyResearchCandidate {
    let config = sample_strategy_config_for_candidate(
        strategy_id,
        symbol,
        timeframe,
        StrategyMode::Paper,
        5,
    );
    StrategyResearchCandidate {
        id,
        strategy_id: config.strategy_id.to_string(),
        symbol: symbol.to_string(),
        timeframe: timeframe.as_str().to_string(),
        config: serde_json::to_value(&config).expect("config should serialize"),
        source_type,
        source_id: Some(Uuid::new_v4()),
        evidence: StrategyResearchCandidateEvidence {
            experiment_id: Some(Uuid::new_v4()),
            experiment_run_id: Some(Uuid::new_v4()),
            walk_forward_id: None,
            pnl_pct: Some(Decimal::new(425, 2)),
            max_drawdown_pct: Some(Decimal::new(125, 2)),
            win_rate: Some(Decimal::new(63, 0)),
            trade_count: Some(17),
            fee_paid: Some(Decimal::new(750, 0)),
            slippage_cost: Some(Decimal::new(125, 0)),
            robustness_score: Some(Decimal::new(72, 2)),
            profitable_windows: Some(4),
            losing_windows: Some(1),
            skipped_windows: Some(0),
            notes: Some("fixture".to_string()),
        },
        score: StrategyResearchCandidateScore {
            score: Decimal::new(8125, 2),
            warnings: vec!["watch_turnover".to_string()],
            rejection_hints: Vec::new(),
        },
        status,
        created_at,
        promoted_at: None,
        promoted_by: None,
        correlation_id: Some(Uuid::new_v4()),
    }
}

fn sample_strategy_walk_forward_request() -> StrategyWalkForwardRequest {
    StrategyWalkForwardRequest {
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        config: None,
        experiment_run_id: None,
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
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_breakdown_pct: None,
            min_reclaim_close_pct: None,
            min_lower_wick_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            holding_candles: Some(3),
            stop_loss_pct: None,
            take_profit_pct: None,
            max_signal_age_ms: Some(180_000),
        },
        min_required_test_windows: Some(2),
        correlation_id: Some(Uuid::from_u128(0x9901)),
    }
}

fn sample_lifecycle_candidate(id: Uuid, created_at: chrono::DateTime<Utc>) -> ResearchCandidate {
    ResearchCandidate {
        id,
        experiment_id: Some(Uuid::new_v4()),
        experiment_run_id: Some(Uuid::new_v4()),
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        config: json!({ "lookback_candles": 20, "holding_candles": 3 }),
        score: Some(Decimal::new(8750, 2)),
        pnl_pct: Some(Decimal::new(245, 2)),
        max_drawdown_pct: Some(Decimal::new(95, 2)),
        trade_count: Some(18),
        win_rate: Some(Decimal::new(57, 2)),
        fee_drag: Some(Decimal::new(15, 2)),
        status: ResearchCandidateStatus::Discovered,
        rejection_reason: None,
        notes: Some("fixture".to_string()),
        created_at,
        updated_at: created_at,
        correlation_id: Some(Uuid::new_v4()),
    }
}

fn sample_shadow_run(
    decision: &str,
    status: &str,
    created_at: chrono::DateTime<Utc>,
) -> TestnetShadowRunRecord {
    TestnetShadowRunRecord {
        id: Uuid::new_v4(),
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        decision: decision.to_string(),
        signal_id: None,
        risk_decision_id: None,
        would_submit_payload: None,
        price_source: None,
        resolved_price: None,
        reasons: Vec::new(),
        status: status.to_string(),
        evaluated_candle_open_time: None,
        created_at,
        correlation_id: Some(Uuid::new_v4()),
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
        failed_windows: 0,
        skipped_windows: 1,
        profitable_test_windows: 1,
        profitable_windows: 1,
        losing_test_windows: 1,
        losing_windows: 1,
        avg_test_pnl_pct: Decimal::new(15, 1),
        avg_pnl_pct: Decimal::new(15, 1),
        median_test_pnl_pct: Decimal::new(15, 1),
        median_pnl_pct: Decimal::new(15, 1),
        worst_test_pnl_pct: Decimal::new(-1, 0),
        worst_pnl_pct: Decimal::new(-1, 0),
        best_test_pnl_pct: Decimal::new(4, 0),
        best_pnl_pct: Decimal::new(4, 0),
        avg_max_drawdown_pct: Decimal::new(25, 1),
        max_drawdown_pct: Decimal::new(3, 0),
        avg_trade_count: Decimal::new(45, 1),
        robustness_score: Decimal::new(42, 1),
        consistency_score: Decimal::new(42, 1),
        status: StrategyWalkForwardStatus::Completed,
        robustness_status: aegis_core::StrategyWalkForwardRobustnessStatus::OverfitRisk,
        robustness_summary: StrategyWalkForwardRobustnessSummary {
            profitable_window_pct: Decimal::new(50, 0),
            total_trade_count: 9,
            avg_trades_per_completed_window: Decimal::new(45, 1),
            avg_fee_slippage_drag_pct: Decimal::new(15, 2),
            skipped_window_pct: Decimal::new(3333, 2),
            dominant_winner_share_pct: Decimal::new(60, 0),
            recommendation: aegis_core::StrategyWalkForwardRecommendation::default(),
        },
        recommendation: aegis_core::StrategyWalkForwardRecommendation::default(),
        warnings: Vec::new(),
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
            raw_signal_count: 6,
            cooldown_suppressed_count: 0,
            open_position_suppressed_count: 1,
            executed_trade_count: 5,
            suppression_breakdown: Vec::new(),
            last_signal_time: Some(fixed_time()),
            last_executed_entry_time: Some(fixed_time()),
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
            raw_signal_count: 5,
            cooldown_suppressed_count: 1,
            open_position_suppressed_count: 0,
            executed_trade_count: 4,
            suppression_breakdown: Vec::new(),
            last_signal_time: Some(fixed_time()),
            last_executed_entry_time: Some(fixed_time()),
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
            raw_signal_count: 0,
            cooldown_suppressed_count: 0,
            open_position_suppressed_count: 0,
            executed_trade_count: 0,
            suppression_breakdown: Vec::new(),
            last_signal_time: None,
            last_executed_entry_time: None,
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
        evaluated_candle_open_time: None,
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
async fn market_data_repair_run_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let run_id = Uuid::new_v4();
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let end = start + chrono::Duration::minutes(5);
    let plan = MarketDataRepairPlan {
        exchange: MarketDataSource::Binance,
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: start,
        end_time: end,
        status: MarketDataRepairStatus::RepairPlanned,
        initial_quality_status: MarketDataQualityStatus::Degraded,
        gap_count: 1,
        repair_ranges: vec![aegis_core::MarketDataRepairRange {
            source_interval: "1m".to_string(),
            start_time: start + chrono::Duration::minutes(2),
            end_time: start + chrono::Duration::minutes(3),
            missing_candle_count: 1,
        }],
        estimated_source_interval: Some("1m".to_string()),
        requires_source_interval: false,
        reaggregate_derived_intervals: false,
        findings: Vec::new(),
        recommendations: Vec::new(),
        correlation_id: Some(Uuid::new_v4()),
    };

    insert_market_data_repair_run(&test_db.pool, run_id, &plan, fixed_time())
        .await
        .expect("repair run should persist");

    let result = MarketDataRepairRunResult {
        run_id,
        plan: plan.clone(),
        status: MarketDataRepairStatus::RepairCompleted,
        before_quality_status: MarketDataQualityStatus::Degraded,
        after_quality_status: MarketDataQualityStatus::Good,
        gap_count_before: 1,
        gap_count_after: 0,
        attempted_ranges: plan.repair_ranges.clone(),
        inserted_candles: 1,
        updated_candles: 0,
        skipped_candles: 0,
        failed_ranges: 0,
        provider_attempts: Vec::new(),
        selected_provider: Some("https://api.binance.com".to_string()),
        aggregation_result: None,
        recommendations: Vec::new(),
        correlation_id: plan.correlation_id,
        created_at: fixed_time(),
        completed_at: Some(fixed_time()),
    };
    let completed = complete_market_data_repair_run(&test_db.pool, run_id, &result)
        .await
        .expect("repair run should complete");
    let loaded = market_data_repair_result_from_record(&completed)
        .expect("repair result should map from record");

    assert_eq!(loaded.run_id, run_id);
    assert_eq!(loaded.status, MarketDataRepairStatus::RepairCompleted);
    assert_eq!(loaded.after_quality_status, MarketDataQualityStatus::Good);
    assert_eq!(loaded.inserted_candles, 1);
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
async fn candle_quality_report_detects_gap_and_is_read_only() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    for index in [0_i64, 1, 3] {
        upsert_candle(&test_db.pool, &sample_backtest_candle(index, 100 + index))
            .await
            .expect("candle persists");
    }

    let request = MarketDataQualityRequest {
        exchange: MarketDataSource::Binance,
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 4, 0).unwrap(),
        expected_interval_seconds: None,
        max_allowed_gap_count: None,
        max_allowed_gap_pct: None,
    };
    let before_paper_orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_orders")
        .fetch_one(&test_db.pool)
        .await
        .expect("paper order count should work");
    let before_testnet_orders: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM exchange_testnet_orders")
            .fetch_one(&test_db.pool)
            .await
            .expect("testnet order count should work");

    let candles = get_candles_for_quality_report(&test_db.pool, &request)
        .await
        .expect("quality candles should load");
    let report = summarize_candle_continuity_report(&test_db.pool, &request)
        .await
        .expect("quality report should build");

    let after_paper_orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_orders")
        .fetch_one(&test_db.pool)
        .await
        .expect("paper order count should work");
    let after_testnet_orders: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM exchange_testnet_orders")
            .fetch_one(&test_db.pool)
            .await
            .expect("testnet order count should work");

    assert_eq!(candles.len(), 3);
    assert_eq!(report.status, MarketDataQualityStatus::Bad);
    assert_eq!(report.gap_count, 1);
    assert_eq!(report.gaps.len(), 1);
    assert_eq!(report.gaps[0].missing_candle_count, 1);
    assert_eq!(before_paper_orders, after_paper_orders);
    assert_eq!(before_testnet_orders, after_testnet_orders);
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
async fn research_coverage_reads_persisted_candles() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    for minute in 0..3 {
        let candle = sample_backtest_candle(minute, 100 + minute);
        upsert_candle(&test_db.pool, &candle)
            .await
            .expect("candle should persist");
    }

    let open_times = list_closed_candle_open_times_in_range(
        &test_db.pool,
        MarketDataSource::Binance,
        &Symbol::new("BTCUSDT").unwrap(),
        CandleInterval::OneMinute,
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 10, 0).unwrap(),
    )
    .await
    .expect("research coverage open_times should load");

    assert_eq!(open_times.len(), 3);
    assert_eq!(
        open_times[0],
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_dataset_build_records_round_trip() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let build_id = Uuid::from_u128(0x778);
    let correlation_id = Uuid::from_u128(0x779);
    let coverage = sample_research_coverage_result();
    let request = ResearchDatasetBuildRequest {
        exchange: MarketDataSource::Binance,
        symbol: "BTCUSDT".to_string(),
        intervals: vec!["1m".to_string(), "5m".to_string()],
        start_time: coverage.window_start,
        end_time: coverage.window_end,
        required_coverage_pct: Decimal::new(95, 0),
        correlation_id: Some(correlation_id),
    };

    insert_research_dataset_build(
        &test_db.pool,
        build_id,
        &request,
        &coverage,
        correlation_id,
        fixed_time(),
    )
    .await
    .expect("research dataset build should persist");

    replace_research_dataset_build_steps(
        &test_db.pool,
        build_id,
        &[ResearchDatasetBuildStep {
            step: "check_and_backfill_1m".to_string(),
            status: ResearchDatasetBuildStepStatus::Completed,
            details: Some(json!({ "inserted_candles": 10 })),
            started_at: fixed_time(),
            completed_at: Some(fixed_time()),
        }],
    )
    .await
    .expect("research dataset build steps should persist");

    let record = get_research_dataset_build(&test_db.pool, build_id)
        .await
        .expect("build lookup should succeed")
        .expect("build should exist");
    let steps = list_research_dataset_build_steps(&test_db.pool, build_id)
        .await
        .expect("step lookup should succeed");
    let build = research_dataset_build_result_from_records(&record, &steps)
        .expect("build result should map");

    assert_eq!(build.status, ResearchDatasetBuildStatus::Started);
    assert_eq!(
        build.coverage_before.status,
        ResearchDataReadinessStatus::Ready
    );
    assert_eq!(build.steps.len(), 1);
    assert_eq!(
        build.steps[0].status,
        ResearchDatasetBuildStepStatus::Completed
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_hypotheses_persist_decide_and_do_not_mutate_execution_tables() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let before_execution = execution_table_counts(&test_db.pool).await;
    let hypothesis = ResearchHypothesis {
        id: Some(Uuid::from_u128(0x4848)),
        source_type: ResearchHypothesisSource::CampaignFailureAttribution,
        status: ResearchHypothesisStatus::Proposed,
        strategy_id: Some("range_reversion_v1".to_string()),
        symbol: Some("BTCUSDT".to_string()),
        timeframe: Some("15m".to_string()),
        regime: Some(ResearchRegimeLabel::Range),
        failure_reasons: vec![aegis_core::ResearchCandidateFailureReason::OverfitRisk],
        evidence: ResearchHypothesisEvidence {
            summary: "overfit risk sample".to_string(),
            details: serde_json::json!({ "source": "integration_db" }),
        },
        recommendation: ResearchHypothesisRecommendation {
            code: "broaden_walk_forward_validation".to_string(),
            actions: vec!["require broader walk-forward".to_string()],
        },
        proposed_action: "Broaden walk-forward validation.".to_string(),
        proposed_experiment_config: serde_json::json!({ "experiment": "broader_walk_forward" }),
        priority: ResearchHypothesisPriority::High,
        expected_effect: "Separate robust behavior from overfit.".to_string(),
        risk: "May reject all candidates.".to_string(),
        created_at: fixed_time(),
    };

    let persisted = insert_research_hypothesis(&test_db.pool, &hypothesis, None)
        .await
        .expect("hypothesis should persist");
    let hypothesis_id = persisted.id.expect("persisted id");
    let fetched = get_research_hypothesis(&test_db.pool, hypothesis_id)
        .await
        .expect("get should work")
        .expect("hypothesis should exist");
    assert_eq!(fetched.status, ResearchHypothesisStatus::Proposed);

    let listed = list_research_hypotheses(&test_db.pool, 10)
        .await
        .expect("list should work");
    assert!(listed.iter().any(|value| value.id == Some(hypothesis_id)));

    let decided = decide_research_hypothesis(
        &test_db.pool,
        hypothesis_id,
        ResearchHypothesisStatus::AcceptedForExperiment,
        Some("integration decision"),
        None,
        None,
    )
    .await
    .expect("decision should persist")
    .expect("hypothesis should exist");
    assert_eq!(
        decided.status,
        ResearchHypothesisStatus::AcceptedForExperiment
    );
    assert_eq!(
        before_execution,
        execution_table_counts(&test_db.pool).await
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
        raw_signal_count: 2,
        cooldown_suppressed_count: 0,
        open_position_suppressed_count: 1,
        executed_trade_count: 1,
        suppression_breakdown: Vec::new(),
        last_signal_time: Some(fixed_time()),
        last_executed_entry_time: Some(fixed_time()),
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
            evaluated_candle_open_time: None,
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
            raw_signal_count: 2,
            cooldown_suppressed_count: 0,
            open_position_suppressed_count: 0,
            executed_trade_count: 2,
            suppression_breakdown: Vec::new(),
            last_signal_time: Some(fixed_time()),
            last_executed_entry_time: Some(fixed_time()),
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
            raw_signal_count: 2,
            cooldown_suppressed_count: 0,
            open_position_suppressed_count: 0,
            executed_trade_count: 2,
            suppression_breakdown: Vec::new(),
            last_signal_time: Some(fixed_time()),
            last_executed_entry_time: Some(fixed_time()),
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
                raw_signal_count: 1,
                cooldown_suppressed_count: 0,
                open_position_suppressed_count: 0,
                executed_trade_count: 1,
                suppression_breakdown: Vec::new(),
                last_signal_time: Some(fixed_time()),
                last_executed_entry_time: Some(fixed_time()),
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
async fn stale_research_campaign_batch_recovery_is_research_only() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let now = fixed_time();
    let campaign_id = Uuid::new_v4();
    let stale_batch_id = Uuid::new_v4();
    let completed_batch_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO research_campaigns (
            id, request, status, summary, created_at, completed_at, correlation_id, error
        )
        VALUES ($1, '{}'::jsonb, 'COMPLETED', '{}'::jsonb, $2, $3, $4, NULL)
        "#,
    )
    .bind(campaign_id)
    .bind(now - chrono::Duration::hours(3))
    .bind(now - chrono::Duration::hours(2))
    .bind(Uuid::new_v4())
    .execute(&test_db.pool)
    .await
    .expect("campaign should insert");

    for (id, status, created_at, completed_at) in [
        (
            stale_batch_id,
            "STARTED",
            now - chrono::Duration::hours(2),
            None,
        ),
        (
            completed_batch_id,
            "COMPLETED",
            now - chrono::Duration::hours(2),
            Some(now - chrono::Duration::hours(1)),
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO research_campaign_batches (
                id, campaign_id, research_batch_id, plan_index, strategy_id, symbol, timeframe,
                window_start, window_end, status, triage_status, candidates_created,
                candidates_blocked_by_gate, proposals_created, summary, error, created_at,
                completed_at
            )
            VALUES (
                $1, $2, NULL, 0, 'failed_breakdown_reclaim_v1', 'ETHUSDT', '15m',
                $3, $4, $5, 'UNKNOWN', 0, 0, 0, '{}'::jsonb, NULL, $6, $7
            )
            "#,
        )
        .bind(id)
        .bind(campaign_id)
        .bind(now - chrono::Duration::days(2))
        .bind(now - chrono::Duration::days(1))
        .bind(status)
        .bind(created_at)
        .bind(completed_at)
        .execute(&test_db.pool)
        .await
        .expect("campaign batch should insert");
    }

    let execution_counts_before = execution_table_counts(&test_db.pool).await;
    let preview = recover_stale_research_runs_at(
        &test_db.pool,
        &ResearchStaleRunRecoveryRequest {
            older_than_minutes: 60,
            dry_run: true,
            target_types: Some(vec![
                ResearchStaleRunRecoveryTargetType::ResearchCampaignBatch,
            ]),
            limit: None,
            correlation_id: Some(Uuid::new_v4()),
            confirmation: None,
        },
        None,
        now,
    )
    .await
    .expect("preview should succeed");

    assert_eq!(preview.scanned_count, 1);
    assert_eq!(preview.stale_count, 1);
    assert_eq!(preview.recovered_count, 0);
    assert_eq!(preview.targets[0].target_id, stale_batch_id);
    let stale_status_after_preview: String =
        sqlx::query_scalar("SELECT status FROM research_campaign_batches WHERE id = $1")
            .bind(stale_batch_id)
            .fetch_one(&test_db.pool)
            .await
            .expect("status should query");
    assert_eq!(stale_status_after_preview, "STARTED");

    let recovered = recover_stale_research_runs_at(
        &test_db.pool,
        &ResearchStaleRunRecoveryRequest {
            older_than_minutes: 60,
            dry_run: false,
            target_types: Some(vec![
                ResearchStaleRunRecoveryTargetType::ResearchCampaignBatch,
            ]),
            limit: None,
            correlation_id: Some(Uuid::new_v4()),
            confirmation: Some("RECOVER STALE RESEARCH RUNS".to_string()),
        },
        None,
        now,
    )
    .await
    .expect("recovery should succeed");

    assert_eq!(recovered.recovered_count, 1);
    let stale_status_after_recovery: String =
        sqlx::query_scalar("SELECT status FROM research_campaign_batches WHERE id = $1")
            .bind(stale_batch_id)
            .fetch_one(&test_db.pool)
            .await
            .expect("status should query");
    assert_eq!(stale_status_after_recovery, "FAILED");
    let completed_status: String =
        sqlx::query_scalar("SELECT status FROM research_campaign_batches WHERE id = $1")
            .bind(completed_batch_id)
            .fetch_one(&test_db.pool)
            .await
            .expect("completed status should query");
    assert_eq!(completed_status, "COMPLETED");
    let recovery_records: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM research_stale_run_recoveries WHERE target_id = $1",
    )
    .bind(stale_batch_id)
    .fetch_one(&test_db.pool)
    .await
    .expect("recovery records should query");
    assert_eq!(recovery_records, 1);
    assert_eq!(
        execution_counts_before,
        execution_table_counts(&test_db.pool).await
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
async fn strategy_robustness_matrix_run_and_cells_persist_in_order() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let run_id = Uuid::new_v4();
    let created_at = fixed_time();
    let first_window = StrategyRobustnessMatrixWindow {
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap(),
    };
    let second_window = StrategyRobustnessMatrixWindow {
        start_time: Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap(),
        end_time: Utc.with_ymd_and_hms(2026, 1, 1, 2, 0, 0).unwrap(),
    };
    let request = StrategyRobustnessMatrixRequest {
        strategy_ids: vec!["trend_filter_momentum_v2".to_string()],
        symbols: vec!["BTCUSDT".to_string()],
        timeframes: vec!["15m".to_string()],
        windows: vec![first_window.clone(), second_window.clone()],
        start_time: None,
        end_time: None,
        window_hours: None,
        step_hours: None,
        config_json_by_strategy: None,
        experiment_run_id: None,
        initial_capital: Decimal::new(1_000_000, 0),
        fee_bps: Decimal::new(10, 0),
        slippage_bps: Decimal::new(5, 0),
        holding_candles: Some(10),
        min_trades_per_cell: 5,
        min_profitable_window_ratio: Decimal::new(50, 2),
    };
    let summary = StrategyRobustnessMatrixStrategySummary {
        strategy_id: "trend_filter_momentum_v2".to_string(),
        status: StrategyRobustnessMatrixStatus::PromisingButWeak,
        profitable_window_ratio: Decimal::new(50, 2),
        avg_pnl_pct: Decimal::new(15, 2),
        median_pnl_pct: Decimal::new(15, 2),
        worst_window_pnl_pct: Decimal::new(-10, 2),
        best_window_pnl_pct: Decimal::new(40, 2),
        avg_trade_count: Decimal::new(6, 0),
        regime_consistency: Decimal::new(50, 2),
        data_quality_penalty: Decimal::ZERO,
        robustness_score: Decimal::new(55, 0),
        completed_cells: 2,
        insufficient_data_cells: 0,
        failed_cells: 0,
        best_symbol: Some("BTCUSDT".to_string()),
        worst_symbol: Some("BTCUSDT".to_string()),
        best_regime: Some(ResearchRegimeLabel::Range),
        worst_regime: Some(ResearchRegimeLabel::Range),
        findings: Vec::new(),
        recommendations: vec![StrategyRobustnessMatrixRecommendation {
            priority: "LOW".to_string(),
            code: "do_not_auto_promote".to_string(),
            message: "Use the matrix as decision support only.".to_string(),
        }],
    };
    let result = StrategyRobustnessMatrixResult {
        run_id,
        status: StrategyRobustnessMatrixStatus::PromisingButWeak,
        request,
        strategy_rankings: vec![summary],
        findings: vec![StrategyRobustnessMatrixFinding {
            severity: "LOW".to_string(),
            code: "promising_strategy".to_string(),
            message: "Strategy has positive cross-window evidence.".to_string(),
        }],
        recommendations: Vec::new(),
        cell_count: 2,
        created_at,
    };
    let mut cells = vec![
        StrategyRobustnessMatrixCell {
            id: Uuid::new_v4(),
            matrix_run_id: run_id,
            strategy_id: "trend_filter_momentum_v2".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            window_start: second_window.start_time,
            window_end: second_window.end_time,
            regime_label: ResearchRegimeLabel::Range,
            data_quality_status: MarketDataQualityStatus::Good,
            status: StrategyRobustnessMatrixStatus::Negative,
            pnl_pct: Decimal::new(-10, 2),
            trade_count: 5,
            raw_signal_count: 8,
            executed_trade_count: 5,
            cooldown_suppressed_count: 1,
            win_rate: Decimal::new(40, 2),
            max_drawdown_pct: Decimal::new(12, 2),
            fee_drag: Decimal::new(3, 2),
            findings: Vec::new(),
            created_at,
        },
        StrategyRobustnessMatrixCell {
            id: Uuid::new_v4(),
            matrix_run_id: run_id,
            strategy_id: "trend_filter_momentum_v2".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            window_start: first_window.start_time,
            window_end: first_window.end_time,
            regime_label: ResearchRegimeLabel::Range,
            data_quality_status: MarketDataQualityStatus::Good,
            status: StrategyRobustnessMatrixStatus::PromisingButWeak,
            pnl_pct: Decimal::new(40, 2),
            trade_count: 7,
            raw_signal_count: 9,
            executed_trade_count: 7,
            cooldown_suppressed_count: 2,
            win_rate: Decimal::new(57, 2),
            max_drawdown_pct: Decimal::new(4, 2),
            fee_drag: Decimal::new(4, 2),
            findings: Vec::new(),
            created_at,
        },
    ];

    insert_strategy_robustness_matrix_run(&test_db.pool, &result)
        .await
        .expect("matrix run should persist");
    insert_strategy_robustness_matrix_cells(&test_db.pool, &cells)
        .await
        .expect("matrix cells should persist");

    let listed_runs = list_strategy_robustness_matrix_runs(&test_db.pool, 10)
        .await
        .expect("matrix runs should list");
    assert!(listed_runs.iter().any(|record| record.id == run_id));

    let run_record = get_strategy_robustness_matrix_run(&test_db.pool, run_id)
        .await
        .expect("matrix run should load")
        .expect("matrix run should exist");
    let mapped_result =
        strategy_robustness_matrix_result_from_record(&run_record).expect("result should map");
    assert_eq!(mapped_result.run_id, run_id);
    assert_eq!(
        mapped_result.strategy_rankings[0].strategy_id,
        "trend_filter_momentum_v2"
    );

    let listed_cells = list_strategy_robustness_matrix_cells(&test_db.pool, run_id)
        .await
        .expect("matrix cells should list");
    assert_eq!(listed_cells.len(), 2);
    assert_eq!(listed_cells[0].window_start, first_window.start_time);
    assert_eq!(listed_cells[1].window_start, second_window.start_time);

    let mapped_cell =
        strategy_robustness_matrix_cell_from_record(&listed_cells[0]).expect("cell should map");
    assert_eq!(mapped_cell.matrix_run_id, run_id);
    assert_eq!(
        mapped_cell.status,
        StrategyRobustnessMatrixStatus::PromisingButWeak
    );

    cells.sort_by_key(|cell| cell.window_start);
    assert_eq!(mapped_cell.id, cells[0].id);
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
                evaluated_candle_open_time: None,
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
            evaluated_candle_open_time: None,
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

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_candidate_persists_manual_fixture_fields() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::Manual,
        StrategyResearchCandidateStatus::Registered,
        fixed_time(),
    );

    let row = insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");
    assert_eq!(row.strategy_id, "momentum_v1");
    assert_eq!(row.symbol, "BTCUSDT");
    assert_eq!(row.timeframe, "15m");
    assert_eq!(row.score, Decimal::new(8125, 2));
    assert_eq!(row.status, "REGISTERED");

    let hydrated =
        strategy_research_candidate_from_record(&row).expect("candidate should deserialize");

    assert_eq!(hydrated.id, candidate.id);
    assert_eq!(hydrated.config, candidate.config);
    assert_eq!(hydrated.evidence, candidate.evidence);
    assert_eq!(hydrated.score.score, candidate.score.score);
    assert_eq!(hydrated.score.warnings, candidate.score.warnings);
    assert_eq!(hydrated.status, StrategyResearchCandidateStatus::Registered);
    assert_eq!(
        hydrated.source_type,
        StrategyResearchCandidateSource::Manual
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_candidate_list_filters_match_expected_rows() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let registered_btc = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "SOLUSDT",
        CandleInterval::OneMinute,
        StrategyResearchCandidateSource::ExperimentRun,
        StrategyResearchCandidateStatus::Registered,
        fixed_time(),
    );
    let registered_eth = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "ADAUSDT",
        CandleInterval::OneMinute,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::Registered,
        fixed_time() + chrono::Duration::seconds(1),
    );
    let promoted_btc = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::VolatilityBreakoutV1,
        "XRPUSDT",
        CandleInterval::OneHour,
        StrategyResearchCandidateSource::Manual,
        StrategyResearchCandidateStatus::Registered,
        fixed_time() + chrono::Duration::seconds(2),
    );

    insert_strategy_research_candidate(&test_db.pool, &registered_btc, None)
        .await
        .expect("registered btc should persist");
    insert_strategy_research_candidate(&test_db.pool, &registered_eth, None)
        .await
        .expect("registered eth should persist");
    insert_strategy_research_candidate(&test_db.pool, &promoted_btc, None)
        .await
        .expect("promoted fixture should persist");
    mark_strategy_research_candidate_promoted(
        &test_db.pool,
        promoted_btc.id,
        None,
        fixed_time() + chrono::Duration::minutes(1),
        None,
    )
    .await
    .expect("promotion marker should persist");

    let by_strategy = list_strategy_research_candidates(
        &test_db.pool,
        &StrategyResearchCandidateListFilters {
            strategy_id: Some("volatility_breakout_v1".to_string()),
            ..StrategyResearchCandidateListFilters::default()
        },
        20,
    )
    .await
    .expect("strategy filter should succeed");
    assert_eq!(by_strategy.len(), 1);
    assert_eq!(by_strategy[0].id, promoted_btc.id);

    let by_symbol = list_strategy_research_candidates(
        &test_db.pool,
        &StrategyResearchCandidateListFilters {
            symbol: Some("xrpusdt".to_string()),
            ..StrategyResearchCandidateListFilters::default()
        },
        20,
    )
    .await
    .expect("symbol filter should succeed");
    assert_eq!(by_symbol.len(), 1);
    assert_eq!(by_symbol[0].id, promoted_btc.id);

    let by_timeframe = list_strategy_research_candidates(
        &test_db.pool,
        &StrategyResearchCandidateListFilters {
            timeframe: Some("1h".to_string()),
            ..StrategyResearchCandidateListFilters::default()
        },
        20,
    )
    .await
    .expect("timeframe filter should succeed");
    assert_eq!(by_timeframe.len(), 1);
    assert_eq!(by_timeframe[0].id, promoted_btc.id);

    let by_status = list_strategy_research_candidates(
        &test_db.pool,
        &StrategyResearchCandidateListFilters {
            status: Some("PROMOTED_TO_SHADOW_CONFIG".to_string()),
            ..StrategyResearchCandidateListFilters::default()
        },
        20,
    )
    .await
    .expect("status filter should succeed");
    assert_eq!(by_status.len(), 1);
    assert_eq!(by_status[0].id, promoted_btc.id);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_candidate_get_returns_exact_detail_payload() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::Registered,
        fixed_time(),
    );

    let row = insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");

    assert_eq!(row.id, candidate.id);
    assert_eq!(row.strategy_id, candidate.strategy_id);
    assert_eq!(row.symbol, candidate.symbol);
    assert_eq!(row.timeframe, candidate.timeframe);
    assert_eq!(row.config, candidate.config);
    assert_eq!(row.source_type, "WALK_FORWARD");

    let hydrated =
        strategy_research_candidate_from_record(&row).expect("candidate should deserialize");
    assert_eq!(hydrated.evidence, candidate.evidence);
    assert_eq!(hydrated.correlation_id, candidate.correlation_id);
}

fn sample_candidate_observation(
    candidate: &StrategyResearchCandidate,
) -> StrategyCandidateObservationResult {
    let evaluated_at = fixed_time() + chrono::Duration::hours(24);
    let requirements = StrategyCandidateObservationRequirement {
        candidate_id: candidate.id,
        strategy_id: candidate.strategy_id.clone(),
        symbol: candidate.symbol.clone(),
        timeframe: candidate.timeframe.clone(),
        min_observation_hours: 24,
        min_shadow_runs: 30,
        max_risk_rejection_rate: Some(Decimal::new(2, 1)),
        min_would_submit_count: 1,
        max_no_signal_rate: Some(Decimal::new(6, 1)),
        require_readiness_ready: true,
    };
    let summary = StrategyCandidateObservationSummary {
        candidate_id: candidate.id,
        window_start: fixed_time(),
        window_end: evaluated_at,
        shadow_runs: 30,
        would_submit_count: 3,
        no_signal_count: 6,
        risk_rejected_count: 3,
        skipped_count: 1,
        risk_rejection_rate: Decimal::new(1, 1),
        no_signal_rate: Decimal::new(2, 1),
        latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
        latest_readiness_score: Some(93),
        runner_alignment: StrategyCandidateRunnerAlignment {
            strategy_config_matches_runner: true,
            runner_enabled: true,
            runner_status: "RUNNING".to_string(),
            runner_timeframe: candidate.timeframe.clone(),
            runner_symbols: vec![candidate.symbol.clone()],
            runner_strategies: vec![candidate.strategy_id.clone()],
            mismatch_reasons: Vec::new(),
        },
        decision: StrategyCandidateObservationDecision::Pass,
        findings: vec![StrategyCandidateObservationFinding {
            code: "requirements_met".to_string(),
            message: "Observation requirements were met.".to_string(),
            blocking: false,
        }],
        recommendations: Vec::new(),
        created_at: evaluated_at,
    };

    StrategyCandidateObservationResult {
        observation_id: Uuid::new_v4(),
        candidate_id: candidate.id,
        strategy_id: candidate.strategy_id.clone(),
        symbol: candidate.symbol.clone(),
        timeframe: candidate.timeframe.clone(),
        status: StrategyCandidateObservationStatus::ReadyForReview,
        requirements,
        runner_alignment: summary.runner_alignment.clone(),
        summary,
        decision: StrategyCandidateObservationDecision::Pass,
        started_at: fixed_time(),
        evaluated_at,
        last_observed_at: evaluated_at,
        observation_expires_at: Some(evaluated_at + chrono::Duration::minutes(15)),
        observation_max_age_seconds: Some(900),
        observation_snapshot_hash: Some("snapshot-hash".to_string()),
        runner_config_snapshot: Some(serde_json::json!({
            "enabled": true,
            "timeframe": candidate.timeframe,
            "symbols": [candidate.symbol],
            "strategies": [candidate.strategy_id],
        })),
        readiness_snapshot: Some(serde_json::json!({
            "status": "READY",
            "score": 93,
        })),
        created_by: None,
        correlation_id: Some(Uuid::new_v4()),
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candidate_observation_persists() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let mut candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        fixed_time(),
    );
    candidate.promoted_at = Some(fixed_time());
    insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");
    let observation = sample_candidate_observation(&candidate);

    let record = insert_strategy_candidate_observation(&test_db.pool, &observation)
        .await
        .expect("observation should persist");
    let hydrated = strategy_candidate_observation_result_from_record(&record)
        .expect("observation should deserialize");

    assert_eq!(hydrated.observation_id, observation.observation_id);
    assert_eq!(
        hydrated.decision,
        StrategyCandidateObservationDecision::Pass
    );
    assert_eq!(
        hydrated.status,
        StrategyCandidateObservationStatus::ReadyForReview
    );
    assert_eq!(hydrated.last_observed_at, observation.last_observed_at);
    assert_eq!(
        hydrated.observation_max_age_seconds,
        observation.observation_max_age_seconds
    );
    assert_eq!(
        hydrated.observation_snapshot_hash,
        observation.observation_snapshot_hash
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candidate_observation_reads_shadow_runs() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    insert_testnet_shadow_run(
        &test_db.pool,
        &TestnetShadowRunRecord {
            id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            decision: "WOULD_SUBMIT".to_string(),
            signal_id: None,
            risk_decision_id: None,
            would_submit_payload: None,
            price_source: Some("local".to_string()),
            resolved_price: Some(Decimal::new(100_000, 0)),
            reasons: Vec::new(),
            status: "COMPLETED".to_string(),
            evaluated_candle_open_time: None,
            created_at: fixed_time() + chrono::Duration::hours(1),
            correlation_id: Some(Uuid::new_v4()),
        },
    )
    .await
    .expect("shadow run should persist");

    let rows = list_testnet_shadow_runs_in_window(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
        fixed_time(),
        fixed_time() + chrono::Duration::hours(2),
    )
    .await
    .expect("shadow runs should load");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].decision, "WOULD_SUBMIT");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn list_testnet_shadow_runs_supports_current_schema() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let run = TestnetShadowRunRecord {
        id: Uuid::new_v4(),
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "1m".to_string(),
        decision: "NO_SIGNAL".to_string(),
        signal_id: None,
        risk_decision_id: None,
        would_submit_payload: None,
        price_source: Some("local".to_string()),
        resolved_price: Some(Decimal::new(100_000, 0)),
        reasons: vec!["insufficient_momentum".to_string()],
        status: "COMPLETED".to_string(),
        evaluated_candle_open_time: None,
        created_at: fixed_time(),
        correlation_id: Some(Uuid::new_v4()),
    };
    insert_testnet_shadow_run(&test_db.pool, &run)
        .await
        .expect("shadow run should persist");

    let listed = list_testnet_shadow_runs(&test_db.pool, 10)
        .await
        .expect("shadow runs should list");
    let fetched = get_testnet_shadow_run_by_id(&test_db.pool, run.id)
        .await
        .expect("shadow run should load")
        .expect("shadow run should exist");

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, run.id);
    assert_eq!(listed[0].reasons, vec!["insufficient_momentum"]);
    assert_eq!(fetched.id, run.id);
    assert_eq!(fetched.decision, "NO_SIGNAL");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn promoted_candidate_links_to_shadow_runs() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let promoted_at = fixed_time();
    let mut candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        promoted_at,
    );
    candidate.promoted_at = Some(promoted_at);
    insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");

    let run = sample_shadow_run(
        "WOULD_SUBMIT",
        "COMPLETED",
        promoted_at + chrono::Duration::minutes(5),
    );
    insert_testnet_shadow_run(&test_db.pool, &run)
        .await
        .expect("shadow run should persist");

    let matched = resolve_promoted_research_candidate_for_shadow_run(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
    )
    .await
    .expect("candidate resolution should succeed");
    assert_eq!(
        matched,
        ShadowRunCandidateMatchOutcome::Matched(candidate.id)
    );

    insert_research_candidate_shadow_run_link(&test_db.pool, candidate.id, run.id, run.created_at)
        .await
        .expect("link insert should succeed")
        .expect("link should be created");

    let linked = list_research_candidate_shadow_runs(
        &test_db.pool,
        candidate.id,
        &ResearchCandidateShadowRunsQuery {
            start_time: promoted_at,
            end_time: promoted_at + chrono::Duration::hours(1),
            limit: 50,
        },
    )
    .await
    .expect("linked runs should list");

    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].shadow_run_id, run.id);
    assert_eq!(linked[0].decision, "WOULD_SUBMIT");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_shadow_pnl_attribution_reads_would_submit_only_without_execution_mutation() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let promoted_at = fixed_time();
    let mut legacy_candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        promoted_at,
    );
    legacy_candidate.promoted_at = Some(promoted_at);
    insert_strategy_research_candidate(&test_db.pool, &legacy_candidate, None)
        .await
        .expect("legacy candidate should persist");
    let candidate = ResearchCandidate {
        id: legacy_candidate.id,
        experiment_id: legacy_candidate.evidence.experiment_id,
        experiment_run_id: legacy_candidate.evidence.experiment_run_id,
        strategy_id: legacy_candidate.strategy_id.clone(),
        symbol: legacy_candidate.symbol.clone(),
        timeframe: legacy_candidate.timeframe.clone(),
        config: legacy_candidate.config.clone(),
        score: Some(legacy_candidate.score.score),
        pnl_pct: legacy_candidate.evidence.pnl_pct,
        max_drawdown_pct: legacy_candidate.evidence.max_drawdown_pct,
        trade_count: legacy_candidate.evidence.trade_count,
        win_rate: legacy_candidate.evidence.win_rate,
        fee_drag: None,
        status: ResearchCandidateStatus::PromotedToShadowConfig,
        rejection_reason: None,
        notes: None,
        created_at: promoted_at,
        updated_at: promoted_at,
        correlation_id: Some(Uuid::new_v4()),
    };

    let would_submit = sample_shadow_run(
        "WOULD_SUBMIT",
        "COMPLETED",
        promoted_at + chrono::Duration::minutes(1),
    );
    let no_signal = sample_shadow_run(
        "NO_SIGNAL",
        "COMPLETED",
        promoted_at + chrono::Duration::minutes(2),
    );
    insert_testnet_shadow_run(&test_db.pool, &would_submit)
        .await
        .expect("would submit run should persist");
    insert_testnet_shadow_run(&test_db.pool, &no_signal)
        .await
        .expect("no signal run should persist");
    insert_research_candidate_shadow_run_link(
        &test_db.pool,
        candidate.id,
        would_submit.id,
        would_submit.created_at,
    )
    .await
    .expect("would submit link")
    .expect("would submit link created");
    insert_research_candidate_shadow_run_link(
        &test_db.pool,
        candidate.id,
        no_signal.id,
        no_signal.created_at,
    )
    .await
    .expect("no signal link")
    .expect("no signal link created");

    for (index, (open, close)) in [(100, 100), (100, 110), (100, 120), (100, 130)]
        .iter()
        .enumerate()
    {
        let open_time = promoted_at + chrono::Duration::minutes(15 * (index as i64 + 1));
        upsert_candle(
            &test_db.pool,
            &Candle {
                id: Uuid::new_v4(),
                exchange: MarketDataSource::Binance,
                symbol: Symbol::new("BTCUSDT").unwrap(),
                interval: CandleInterval::FifteenMinutes,
                open_time,
                close_time: open_time + chrono::Duration::minutes(15),
                open: Decimal::new(*open, 0),
                high: Decimal::new((*open).max(*close), 0),
                low: Decimal::new((*open).min(*close), 0),
                close: Decimal::new(*close, 0),
                volume: Decimal::ONE,
                quote_volume: None,
                trade_count: 1,
                is_closed: true,
                created_at: open_time + chrono::Duration::minutes(15),
                updated_at: open_time + chrono::Duration::minutes(15),
            },
        )
        .await
        .expect("candle should upsert");
    }

    let before_orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(&test_db.pool)
        .await
        .unwrap();
    let before_paper_positions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_positions")
        .fetch_one(&test_db.pool)
        .await
        .unwrap();
    let before_paper_fills: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_fills")
        .fetch_one(&test_db.pool)
        .await
        .unwrap();
    let before_testnet_orders: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM exchange_testnet_orders")
            .fetch_one(&test_db.pool)
            .await
            .unwrap();
    let before_testnet_order_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM exchange_testnet_order_lifecycle_events")
            .fetch_one(&test_db.pool)
            .await
            .unwrap();
    let before_shadow_promotions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM testnet_shadow_promotions")
            .fetch_one(&test_db.pool)
            .await
            .unwrap();

    let attribution = get_research_candidate_shadow_pnl_attribution(
        &test_db.pool,
        &candidate,
        &ResearchShadowPnlAttributionRequest {
            candidate_id: candidate.id,
            holding_windows: vec![1, 3],
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            extreme_pnl_threshold_pct: Decimal::new(5, 0),
            start_time: Some(promoted_at),
            end_time: Some(promoted_at + chrono::Duration::hours(1)),
            limit: Some(50),
        },
        promoted_at + chrono::Duration::hours(1),
    )
    .await
    .expect("attribution should compute");

    assert_eq!(attribution.trades.len(), 1);
    assert_eq!(attribution.trades[0].shadow_run_id, would_submit.id);
    assert_eq!(attribution.trades[0].strategy_id, candidate.strategy_id);
    assert_eq!(attribution.trades[0].symbol, "BTCUSDT");
    assert_eq!(attribution.trades[0].timeframe, "15m");
    assert_eq!(
        attribution.trades[0].entry_price,
        Some(Decimal::new(100, 0))
    );
    assert_eq!(
        attribution.trades[0].holding_windows[0].attribution_status,
        ResearchShadowPnlStatus::ExtremePnl
    );
    assert_eq!(
        attribution.trades[0].holding_windows[0].gross_pnl_pct,
        Some(Decimal::new(10, 0))
    );
    assert_eq!(
        attribution.trades[0].holding_windows[0].net_pnl_pct,
        Some(Decimal::new(985, 2))
    );
    assert_eq!(attribution.summary.extreme_pnl_count, 2);
    assert_eq!(attribution.summary.gap_detected_count, 0);
    assert_eq!(attribution.summary.total_attributed_runs, 1);
    assert_eq!(attribution.summary.insufficient_forward_data_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM orders")
            .fetch_one(&test_db.pool)
            .await
            .unwrap(),
        before_orders
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM paper_positions")
            .fetch_one(&test_db.pool)
            .await
            .unwrap(),
        before_paper_positions
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM paper_fills")
            .fetch_one(&test_db.pool)
            .await
            .unwrap(),
        before_paper_fills
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM exchange_testnet_orders")
            .fetch_one(&test_db.pool)
            .await
            .unwrap(),
        before_testnet_orders
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM exchange_testnet_order_lifecycle_events"
        )
        .fetch_one(&test_db.pool)
        .await
        .unwrap(),
        before_testnet_order_events
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM testnet_shadow_promotions")
            .fetch_one(&test_db.pool)
            .await
            .unwrap(),
        before_shadow_promotions
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn unaccepted_candidate_does_not_link_to_shadow_runs() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::Registered,
        fixed_time(),
    );
    insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");

    let matched = resolve_promoted_research_candidate_for_shadow_run(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
    )
    .await
    .expect("candidate resolution should succeed");

    assert_eq!(matched, ShadowRunCandidateMatchOutcome::NotFound);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn shadow_performance_summary_reads_linked_runs_only() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let promoted_at = fixed_time();
    let mut candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        promoted_at,
    );
    candidate.promoted_at = Some(promoted_at);
    insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");

    let lifecycle_candidate = sample_lifecycle_candidate(candidate.id, promoted_at);
    let linked_run = sample_shadow_run(
        "WOULD_SUBMIT",
        "COMPLETED",
        promoted_at + chrono::Duration::minutes(1),
    );
    let unlinked_run = sample_shadow_run(
        "RISK_REJECTED",
        "REJECTED",
        promoted_at + chrono::Duration::minutes(2),
    );
    insert_testnet_shadow_run(&test_db.pool, &linked_run)
        .await
        .expect("linked shadow run should persist");
    insert_testnet_shadow_run(&test_db.pool, &unlinked_run)
        .await
        .expect("unlinked shadow run should persist");
    insert_research_candidate_shadow_run_link(
        &test_db.pool,
        candidate.id,
        linked_run.id,
        linked_run.created_at,
    )
    .await
    .expect("link insert should succeed");

    let performance = get_research_candidate_shadow_performance(
        &test_db.pool,
        &lifecycle_candidate,
        &ResearchCandidateShadowPerformanceWindow {
            start_time: promoted_at,
            end_time: promoted_at + chrono::Duration::hours(1),
        },
        true,
        promoted_at + chrono::Duration::hours(1),
    )
    .await
    .expect("performance summary should load");

    assert_eq!(performance.total_shadow_runs, 1);
    assert_eq!(performance.would_submit_count, 1);
    assert_eq!(performance.risk_rejected_count, 0);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn ambiguous_candidate_matching_uses_latest_promoted_candidate() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let first_promoted_at = fixed_time();
    let second_promoted_at = fixed_time() + chrono::Duration::minutes(10);
    let mut first = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        first_promoted_at,
    );
    first.promoted_at = Some(first_promoted_at);
    let mut second = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        second_promoted_at,
    );
    second.promoted_at = Some(second_promoted_at);
    insert_strategy_research_candidate(&test_db.pool, &first, None)
        .await
        .expect("first candidate should persist");
    insert_strategy_research_candidate(&test_db.pool, &second, None)
        .await
        .expect("second candidate should persist");

    let matched = resolve_promoted_research_candidate_for_shadow_run(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
    )
    .await
    .expect("candidate resolution should succeed");

    assert_eq!(matched, ShadowRunCandidateMatchOutcome::Matched(second.id));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candidate_matching_is_ambiguous_when_latest_promotions_share_timestamp() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let promoted_at = fixed_time();
    let mut first = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        promoted_at,
    );
    first.promoted_at = Some(promoted_at);
    let mut second = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        promoted_at,
    );
    second.promoted_at = Some(promoted_at);
    insert_strategy_research_candidate(&test_db.pool, &first, None)
        .await
        .expect("first candidate should persist");
    insert_strategy_research_candidate(&test_db.pool, &second, None)
        .await
        .expect("second candidate should persist");

    let matched = resolve_promoted_research_candidate_for_shadow_run(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
    )
    .await
    .expect("candidate resolution should succeed");

    assert_eq!(matched, ShadowRunCandidateMatchOutcome::Ambiguous);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candidate_observation_list_filters_by_candidate() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let mut first = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        fixed_time(),
    );
    first.promoted_at = Some(fixed_time());
    let mut second = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "ETHUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        fixed_time(),
    );
    second.promoted_at = Some(fixed_time());
    insert_strategy_research_candidate(&test_db.pool, &first, None)
        .await
        .expect("first candidate should persist");
    insert_strategy_research_candidate(&test_db.pool, &second, None)
        .await
        .expect("second candidate should persist");
    insert_strategy_candidate_observation(&test_db.pool, &sample_candidate_observation(&first))
        .await
        .expect("first observation should persist");
    insert_strategy_candidate_observation(&test_db.pool, &sample_candidate_observation(&second))
        .await
        .expect("second observation should persist");

    let filtered = list_strategy_candidate_observations(&test_db.pool, first.id)
        .await
        .expect("filtered observations should load");

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].candidate_id, first.id);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn candidate_observation_insert_does_not_mutate_execution_tables() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let mut candidate = sample_research_candidate(
        Uuid::new_v4(),
        StrategyId::MomentumV1,
        "BTCUSDT",
        CandleInterval::FifteenMinutes,
        StrategyResearchCandidateSource::WalkForward,
        StrategyResearchCandidateStatus::PromotedToShadowConfig,
        fixed_time(),
    );
    candidate.promoted_at = Some(fixed_time());
    insert_strategy_research_candidate(&test_db.pool, &candidate, None)
        .await
        .expect("candidate should persist");
    let before_orders = list_orders(&test_db.pool)
        .await
        .expect("orders should list")
        .len();
    let before_signals = list_recent_signals(&test_db.pool, None, 20)
        .await
        .expect("signals should list")
        .len();
    let before_shadow = list_testnet_shadow_runs_in_window(
        &test_db.pool,
        "momentum_v1",
        "BTCUSDT",
        "15m",
        fixed_time(),
        fixed_time() + chrono::Duration::hours(24),
    )
    .await
    .expect("shadow runs should list")
    .len();

    insert_strategy_candidate_observation(&test_db.pool, &sample_candidate_observation(&candidate))
        .await
        .expect("observation should persist");

    assert_eq!(
        list_orders(&test_db.pool)
            .await
            .expect("orders should list")
            .len(),
        before_orders
    );
    assert_eq!(
        list_recent_signals(&test_db.pool, None, 20)
            .await
            .expect("signals should list")
            .len(),
        before_signals
    );
    assert_eq!(
        list_testnet_shadow_runs_in_window(
            &test_db.pool,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            fixed_time(),
            fixed_time() + chrono::Duration::hours(24),
        )
        .await
        .expect("shadow runs should list")
        .len(),
        before_shadow
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_candidate_lifecycle_creation_persists_candidate_and_event() {
    let test_db = TestDatabase::setup().await.expect("db should setup");
    let candidate = sample_lifecycle_candidate(Uuid::new_v4(), fixed_time());

    let (candidate_record, event_record) = create_research_candidate(
        &test_db.pool,
        &candidate,
        None,
        ResearchCandidateDecision::Reopen,
        Some("created"),
        candidate.notes.as_deref(),
        &json!({ "source": "manual" }),
    )
    .await
    .expect("candidate should persist");

    let hydrated = research_candidate_from_record(&candidate_record).expect("candidate should map");
    let event = research_candidate_event_from_record(&event_record).expect("event should map");

    assert_eq!(hydrated.status, ResearchCandidateStatus::Discovered);
    assert_eq!(hydrated.strategy_id, "momentum_v1");
    assert_eq!(event.previous_status, None);
    assert_eq!(event.next_status, ResearchCandidateStatus::Discovered);
    assert_eq!(event.decision, ResearchCandidateDecision::Reopen);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_candidate_lifecycle_events_are_ordered_and_append_on_decision() {
    let test_db = TestDatabase::setup().await.expect("db should setup");
    let candidate = sample_lifecycle_candidate(Uuid::new_v4(), fixed_time());

    let (candidate_record, _) = create_research_candidate(
        &test_db.pool,
        &candidate,
        None,
        ResearchCandidateDecision::Reopen,
        Some("created"),
        candidate.notes.as_deref(),
        &json!({ "source": "manual" }),
    )
    .await
    .expect("candidate should persist");
    let hydrated = research_candidate_from_record(&candidate_record).expect("candidate should map");

    let updated = db::update_research_candidate_status(
        &test_db.pool,
        hydrated.id,
        ResearchCandidateStatus::Rejected,
        Some("bad drawdown"),
        Some("rejecting fixture"),
        fixed_time() + chrono::Duration::minutes(1),
        hydrated.correlation_id,
    )
    .await
    .expect("candidate should update")
    .expect("candidate should exist");
    let updated = research_candidate_from_record(&updated).expect("candidate should re-map");
    assert_eq!(updated.status, ResearchCandidateStatus::Rejected);

    append_research_candidate_event(
        &test_db.pool,
        &ResearchCandidateLifecycleEvent {
            id: Uuid::new_v4(),
            candidate_id: hydrated.id,
            previous_status: Some(ResearchCandidateStatus::Discovered),
            next_status: ResearchCandidateStatus::Rejected,
            decision: ResearchCandidateDecision::Reject,
            reason: Some("bad drawdown".to_string()),
            notes: Some("rejecting fixture".to_string()),
            actor_id: None,
            payload: json!({ "test": true }),
            created_at: fixed_time() + chrono::Duration::minutes(1),
            correlation_id: hydrated.correlation_id,
        },
    )
    .await
    .expect("event should append");

    let events = db::list_research_candidate_events(&test_db.pool, hydrated.id)
        .await
        .expect("events should list");
    assert_eq!(events.len(), 2);
    assert!(events[0].created_at <= events[1].created_at);
    let last = research_candidate_event_from_record(&events[1]).expect("event should map");
    assert_eq!(last.decision, ResearchCandidateDecision::Reject);
    assert_eq!(last.reason.as_deref(), Some("bad drawdown"));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_regime_discovery_and_windows_persist() {
    let test_db = TestDatabase::setup().await.expect("db should setup");
    let discovery_id = Uuid::new_v4();
    let created_at = fixed_time();
    let request = ResearchRegimeDiscoveryRequest {
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        scan_start: created_at,
        scan_end: created_at + chrono::Duration::days(1),
        window_hours: 24,
        step_hours: 12,
        target_regimes: Some(vec![ResearchRegimeLabel::Range]),
        max_windows_per_regime: 1,
        min_confidence: None,
        require_existing_candles: true,
        auto_backfill_missing: false,
        classifier_config: None,
        calibration_id: None,
    };
    let explanation = ResearchRegimeClassificationExplanation {
        return_pct: Decimal::ZERO,
        realized_volatility: Decimal::new(1, 0),
        avg_range_pct: Decimal::new(1, 0),
        trend_slope: Decimal::ZERO,
        choppiness_proxy: Decimal::new(80, 0),
        thresholds_used: ResearchRegimeClassifierConfig::default(),
        conditions: Vec::new(),
        final_label: ResearchRegimeLabel::Range,
        confidence: Decimal::new(90, 0),
        alternate_labels_considered: Vec::new(),
    };
    let window = ResearchRegimeDiscoveryCandidateWindow {
        id: Uuid::new_v4(),
        regime_label: ResearchRegimeLabel::Range,
        start_time: request.scan_start,
        end_time: request.scan_end,
        confidence: Decimal::new(90, 0),
        return_pct: Decimal::ZERO,
        realized_volatility: Decimal::new(1, 0),
        avg_range_pct: Decimal::new(1, 0),
        trend_slope: Decimal::ZERO,
        choppiness_proxy: Decimal::new(80, 0),
        data_quality_status: MarketDataQualityStatus::Good,
        candle_count: 96,
        explanation,
    };
    let summary = ResearchRegimeDiscoverySummary {
        total_windows_scanned: 1,
        selected_window_count: 1,
        counts_by_regime: [(ResearchRegimeLabel::Range, 1)].into_iter().collect(),
        missing_regimes: Vec::new(),
        data_quality_blocked_count: 0,
        insufficient_data_count: 0,
        recommendations: vec![ResearchRegimeDiscoveryRecommendation {
            priority: "LOW".to_string(),
            code: "research_only".to_string(),
            message: "Research only.".to_string(),
        }],
    };
    let result = ResearchRegimeDiscoveryResult {
        discovery_id,
        status: ResearchRegimeDiscoveryStatus::Completed,
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        scan_start: request.scan_start,
        scan_end: request.scan_end,
        total_windows_scanned: 1,
        selected_windows: vec![window],
        counts_by_regime: summary.counts_by_regime.clone(),
        missing_regimes: Vec::new(),
        data_quality_blocked_count: 0,
        recommendations: summary.recommendations.clone(),
        request,
        summary,
        created_at,
    };

    insert_research_regime_discovery(&test_db.pool, &result)
        .await
        .expect("discovery should persist");
    let record = get_research_regime_discovery(&test_db.pool, discovery_id)
        .await
        .expect("discovery should load")
        .expect("discovery should exist");
    let windows = list_research_regime_discovery_windows(&test_db.pool, discovery_id)
        .await
        .expect("windows should load");
    let hydrated =
        research_regime_discovery_result_from_records(&record, &windows).expect("should map");

    assert_eq!(hydrated.discovery_id, discovery_id);
    assert_eq!(hydrated.selected_windows.len(), 1);
    assert_eq!(
        hydrated.selected_windows[0].regime_label,
        ResearchRegimeLabel::Range
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn research_regime_calibration_and_candidates_persist() {
    let test_db = TestDatabase::setup().await.expect("db should setup");
    let calibration_id = Uuid::new_v4();
    let created_at = fixed_time();
    let config = ResearchRegimeClassifierConfig {
        trend_return_threshold_pct: Decimal::new(10, 1),
        trend_slope_threshold: Decimal::ZERO,
        range_return_max_pct: Decimal::new(8, 1),
        range_choppiness_min: Decimal::new(70, 0),
        high_volatility_threshold_pct: Decimal::new(45, 2),
        low_volatility_threshold_pct: Decimal::new(18, 2),
        min_confidence: Decimal::ZERO,
        priority_order: vec![
            ResearchRegimeLabel::HighVolatility,
            ResearchRegimeLabel::TrendUp,
            ResearchRegimeLabel::TrendDown,
            ResearchRegimeLabel::LowVolatility,
            ResearchRegimeLabel::Range,
        ],
    };
    let request = ResearchRegimeCalibrationRequest {
        symbol: "BTCUSDT".to_string(),
        timeframe: "15m".to_string(),
        scan_start: created_at,
        scan_end: created_at + chrono::Duration::days(30),
        window_hours: 24,
        step_hours: 12,
        threshold_candidates: None,
        target_min_windows_per_regime: 10,
    };
    let candidate = ResearchRegimeCalibrationCandidateResult {
        candidate_id: "crypto_vol_balanced".to_string(),
        classifier_config: config.clone(),
        counts_by_regime: [
            (ResearchRegimeLabel::TrendUp, 10),
            (ResearchRegimeLabel::TrendDown, 10),
            (ResearchRegimeLabel::Range, 10),
            (ResearchRegimeLabel::HighVolatility, 10),
            (ResearchRegimeLabel::LowVolatility, 10),
        ]
        .into_iter()
        .collect(),
        missing_regimes: Vec::new(),
        total_windows_scanned: 100,
        data_quality_good_windows: 100,
        avg_confidence: Decimal::new(75, 0),
        diversity_score: Decimal::new(100, 0),
        balance_score: Decimal::new(80, 0),
        dominant_regime_share: Decimal::new(20, 0),
        total_score: Decimal::new(925, 1),
        warnings: Vec::new(),
        explanation_samples: Vec::new(),
    };
    let result = ResearchRegimeCalibrationResult {
        calibration_id,
        status: ResearchRegimeCalibrationStatus::Completed,
        request,
        candidates: vec![candidate],
        recommended_config: Some(config.clone()),
        recommended_candidate_id: Some("crypto_vol_balanced".to_string()),
        missing_regimes: Vec::new(),
        recommendations: vec![ResearchRegimeCalibrationRecommendation {
            priority: "LOW".to_string(),
            code: "research_only".to_string(),
            message: "Research only.".to_string(),
        }],
        created_at,
    };

    insert_research_regime_calibration(&test_db.pool, &result, Some(Uuid::new_v4()))
        .await
        .expect("calibration should persist");
    let record = get_research_regime_calibration(&test_db.pool, calibration_id)
        .await
        .expect("calibration should load")
        .expect("calibration should exist");
    let candidates = list_research_regime_calibration_candidates(&test_db.pool, calibration_id)
        .await
        .expect("candidates should load");
    let hydrated =
        research_regime_calibration_result_from_records(&record, &candidates).expect("should map");
    let listed = list_research_regime_calibrations(&test_db.pool, 10)
        .await
        .expect("calibrations should list");

    assert!(listed.iter().any(|record| record.id == calibration_id));
    assert_eq!(hydrated.calibration_id, calibration_id);
    assert_eq!(hydrated.recommended_config, Some(config));
    assert_eq!(hydrated.candidates.len(), 1);
    assert!(hydrated.candidates[0].missing_regimes.is_empty());
}
