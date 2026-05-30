use std::{net::SocketAddr, time::Duration};

use aegis_core::{
    Candle, CandleInterval, DataFreshnessStatus, FeedStatus, MarketDataSource, MarketMode,
    MarketTick, PaperCloseMode, PaperClosePositionRequest, PaperCloseStatus,
    PaperCloseValidationIssue, PaperPositionStatusFilter, PaperTradingPipelineRequest,
    PipelineDecision, PipelineStepStatus, ResearchCandidate, ResearchCandidateDecision,
    ResearchCandidateStatus, ScheduledResearchJobKind, ScheduledResearchJobRequest, StrategyConfig,
    StrategyId, StrategyMode, Symbol, TestnetShadowRunnerConfigInput,
    TestnetShadowRunnerControlAction, TestnetShadowRunnerStaleFeedPolicy,
    TestnetShadowRunnerStatus, RELATIVE_STRENGTH_CONTINUATION_V1_ID,
};
use api::{
    close_paper_position,
    pipeline::run_paper_pipeline,
    scheduled_research::run_scheduled_research_job_once,
    testnet_shadow_runner::{
        apply_testnet_shadow_runner_control_action, load_testnet_shadow_runner_snapshot,
        persist_testnet_shadow_runner_config, run_shadow_runner_tick, RunnerTickMode,
    },
    AppConfig, AppState, StrategyRuntimeConfig,
};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use db::{
    create_research_candidate, get_default_paper_account, get_order_by_id, get_paper_position,
    get_recent_closed_candles, get_research_candidate, get_risk_decision, insert_market_tick,
    insert_research_candidate_shadow_run_link, insert_scheduled_research_job,
    insert_testnet_shadow_run, list_open_paper_positions, list_orders, list_paper_equity_snapshots,
    list_paper_positions, list_paper_trade_journal, list_recent_signals, list_recent_system_events,
    scheduled_research_job_from_record, set_kill_switch_state, test_support::TestDatabase,
    upsert_candle, upsert_market_feed_status, upsert_strategy_config, StateActor,
    TestnetShadowRunRecord,
};
use market_ingest::MarketIngestConfig;
use rust_decimal::Decimal;
use sqlx::Row;
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
        enabled: true,
        mode: StrategyMode::Paper,
        symbols: vec![Symbol::new("BTCUSDT").expect("valid symbol")],
        timeframe: CandleInterval::OneMinute,
        suggested_notional: Decimal::new(100_000, 0),
        max_signal_age_ms: 180_000,
        cooldown_seconds: 900,
        lookback_candles: 3,
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
        confirmation_candles: 0,
        require_confirmation_close_above_lookback_low: false,
        require_confirmation_low_above_breakdown_low: false,
        notes: None,
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
            shadow_observation_only: true,
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
            binance_rest_fallback_base_urls: Vec::new(),
        },
        strategy_runtime: runtime_config(),
    }
}

async fn execution_table_counts(pool: &db::PgPool) -> Vec<(String, i64)> {
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

#[tokio::test]
#[ignore = "requires Postgres test database"]
async fn scheduled_research_run_once_records_run_and_keeps_execution_tables_unchanged() {
    let db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let state = app_state(db.pool.clone());
    let request = ScheduledResearchJobRequest {
        name: "Aggregation status".to_string(),
        kind: ScheduledResearchJobKind::AggregationStatus,
        enabled: false,
        interval_seconds: 60,
        request: serde_json::json!({"symbols": ["BTCUSDT"], "target_intervals": ["5m"]}),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let job_record = insert_scheduled_research_job(&db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");
    let before = execution_table_counts(&db.pool).await;

    let run = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("run-once should record a run");
    assert_eq!(run.job_id, job.id);
    assert!(run.error.is_none());
    assert_eq!(before, execution_table_counts(&db.pool).await);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn scheduled_candidate_shadow_observe_once_skips_then_records_only_on_new_candle() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let state = app_state(test_db.pool.clone());
    let candidate = seed_fresh_shadow_candidate_fixture(&test_db.pool).await;
    persist_testnet_shadow_runner_config(&state, &runner_config_input(false), None)
        .await
        .expect("runner config should persist");

    let latest_candle = get_recent_closed_candles(
        &test_db.pool,
        &Symbol::new("BTCUSDT").expect("valid symbol"),
        CandleInterval::OneMinute,
        1,
    )
    .await
    .expect("latest candle should load")
    .pop()
    .expect("latest candle should exist");
    let prior_run = TestnetShadowRunRecord {
        id: Uuid::new_v4(),
        strategy_id: candidate.strategy_id.clone(),
        symbol: candidate.symbol.clone(),
        timeframe: candidate.timeframe.clone(),
        decision: "NO_SIGNAL".to_string(),
        signal_id: None,
        risk_decision_id: None,
        would_submit_payload: None,
        price_source: None,
        resolved_price: None,
        reasons: Vec::new(),
        status: "COMPLETED".to_string(),
        evaluated_candle_open_time: Some(latest_candle.open_time),
        created_at: Utc::now(),
        correlation_id: Some(Uuid::new_v4()),
    };
    insert_testnet_shadow_run(&test_db.pool, &prior_run)
        .await
        .expect("prior shadow run should persist");
    insert_research_candidate_shadow_run_link(
        &test_db.pool,
        candidate.id,
        prior_run.id,
        Utc::now(),
    )
    .await
    .expect("prior link should persist");

    let request = ScheduledResearchJobRequest {
        name: "candidate shadow observe once".to_string(),
        kind: ScheduledResearchJobKind::CandidateShadowObserveOnce,
        enabled: false,
        interval_seconds: 300,
        request: serde_json::json!({ "candidate_id": candidate.id }),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let job_record = insert_scheduled_research_job(&test_db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");
    let before_execution = execution_table_counts(&test_db.pool).await;
    let before_shadow_runs = count_rows(&test_db.pool, "testnet_shadow_runs").await;
    let before_links = count_rows(&test_db.pool, "research_candidate_shadow_runs").await;

    let skipped = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("run-once should record skipped run");
    assert_eq!(skipped.status.as_str(), "SKIPPED");
    assert_eq!(
        skipped.result["payload"]["decision"],
        "SKIPPED_NO_NEW_CANDLE"
    );
    assert_eq!(
        count_rows(&test_db.pool, "testnet_shadow_runs").await,
        before_shadow_runs
    );
    assert_eq!(
        count_rows(&test_db.pool, "research_candidate_shadow_runs").await,
        before_links
    );
    assert_eq!(
        execution_table_counts(&test_db.pool).await,
        before_execution
    );

    let new_open_time = latest_candle.open_time + ChronoDuration::minutes(1);
    upsert_candle(
        &test_db.pool,
        &Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            interval: CandleInterval::OneMinute,
            open_time: new_open_time,
            close_time: new_open_time + ChronoDuration::minutes(1),
            open: Decimal::new(104_000, 0),
            high: Decimal::new(105_000, 0),
            low: Decimal::new(103_500, 0),
            close: Decimal::new(104_500, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1_045_000, 0)),
            trade_count: 5,
            is_closed: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    )
    .await
    .expect("new candle should persist");

    let observed = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("run-once should observe new candle");
    assert_eq!(observed.status.as_str(), "COMPLETED");
    assert_eq!(observed.result["payload"]["decision"], "OBSERVED");
    assert_eq!(
        count_rows(&test_db.pool, "testnet_shadow_runs").await,
        before_shadow_runs + 1
    );
    assert_eq!(
        count_rows(&test_db.pool, "research_candidate_shadow_runs").await,
        before_links + 1
    );
    assert_eq!(
        execution_table_counts(&test_db.pool).await,
        before_execution
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn scheduled_cross_asset_candidate_shadow_observe_once_is_unique_candle_and_execution_isolated(
) {
    let test_db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let state = app_state(test_db.pool.clone());
    let candidate = seed_cross_asset_candidate_fixture(&test_db.pool).await;
    let before_execution = execution_table_counts(&test_db.pool).await;
    let before_candidate_status = get_research_candidate(&test_db.pool, candidate.id)
        .await
        .expect("candidate query should succeed")
        .expect("candidate should exist")
        .status;

    let request = ScheduledResearchJobRequest {
        name: "rs-v1-cross-asset-shadow-observe".to_string(),
        kind: ScheduledResearchJobKind::CrossAssetCandidateShadowObserveOnce,
        enabled: false,
        interval_seconds: 900,
        request: serde_json::json!({ "candidate_id": candidate.id }),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let job_record = insert_scheduled_research_job(&test_db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");

    let first = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("first cross-asset run should observe latest aligned candle");
    assert_eq!(first.status.as_str(), "COMPLETED");
    assert_eq!(first.result["payload"]["observation_created"], true);
    assert!(matches!(
        first.result["payload"]["decision"].as_str(),
        Some("NO_SIGNAL" | "WOULD_SELECT")
    ));
    assert_eq!(
        count_rows(&test_db.pool, "cross_asset_candidate_shadow_observations").await,
        1
    );
    assert!(
        count_rows(
            &test_db.pool,
            "cross_asset_candidate_shadow_observation_rankings"
        )
        .await
            > 0
    );

    let duplicate = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("same-candle rerun should skip");
    assert_eq!(duplicate.status.as_str(), "SKIPPED");
    assert_eq!(
        duplicate.result["payload"]["decision"],
        "SKIPPED_NO_NEW_CANDLE"
    );
    assert_eq!(
        count_rows(&test_db.pool, "cross_asset_candidate_shadow_observations").await,
        1
    );

    append_cross_asset_fixture_candle(&test_db.pool).await;
    let second = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("new aligned candle should create one more observation");
    assert_eq!(second.status.as_str(), "COMPLETED");
    assert_eq!(second.result["payload"]["observation_created"], true);
    assert_eq!(
        count_rows(&test_db.pool, "cross_asset_candidate_shadow_observations").await,
        2
    );
    assert_eq!(
        execution_table_counts(&test_db.pool).await,
        before_execution
    );
    assert_eq!(
        get_research_candidate(&test_db.pool, candidate.id)
            .await
            .expect("candidate query should succeed")
            .expect("candidate should exist")
            .status,
        before_candidate_status
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn scheduled_cross_asset_candidate_shadow_observe_once_fails_closed_without_observation_only()
{
    let test_db = TestDatabase::setup()
        .await
        .expect("test database should setup");
    let mut state = app_state(test_db.pool.clone());
    state.config.shadow_observation_only = false;
    let candidate = seed_cross_asset_candidate_fixture(&test_db.pool).await;
    let before_execution = execution_table_counts(&test_db.pool).await;
    let request = ScheduledResearchJobRequest {
        name: "rs-v1-cross-asset-shadow-observe-disabled".to_string(),
        kind: ScheduledResearchJobKind::CrossAssetCandidateShadowObserveOnce,
        enabled: false,
        interval_seconds: 900,
        request: serde_json::json!({ "candidate_id": candidate.id }),
        max_runs_per_tick: 1,
        next_run_at: None,
    };
    let job_record = insert_scheduled_research_job(&test_db.pool, &request)
        .await
        .expect("scheduled job should persist");
    let job = scheduled_research_job_from_record(&job_record).expect("job should map");

    let failed = run_scheduled_research_job_once(&state, &job)
        .await
        .expect("failed-closed run should still record scheduled run");
    assert_eq!(failed.status.as_str(), "FAILED");
    assert!(failed
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("SHADOW_OBSERVATION_ONLY=true"));
    assert_eq!(
        count_rows(&test_db.pool, "cross_asset_candidate_shadow_observations").await,
        0
    );
    assert_eq!(
        execution_table_counts(&test_db.pool).await,
        before_execution
    );
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

async fn seed_fresh_shadow_candidate_fixture(pool: &db::PgPool) -> ResearchCandidate {
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
    insert_market_tick(
        pool,
        &sample_market_tick(Decimal::new(103_500, 0), Utc::now()),
    )
    .await
    .expect("market tick should persist");

    let base_open = Utc::now() - ChronoDuration::seconds(270);
    for index in 0..4 {
        let open_time = base_open + ChronoDuration::minutes(index);
        let close = 100_000_i64 + index * 1_000;
        upsert_candle(
            pool,
            &Candle {
                id: Uuid::new_v4(),
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
            },
        )
        .await
        .expect("fresh candle should persist");
    }

    let candidate = ResearchCandidate {
        id: Uuid::new_v4(),
        experiment_id: None,
        experiment_run_id: None,
        strategy_id: "momentum_v1".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "1m".to_string(),
        config: serde_json::to_value(strategy_config()).expect("strategy config json"),
        score: Some(Decimal::new(100, 0)),
        pnl_pct: Some(Decimal::new(10, 0)),
        max_drawdown_pct: Some(Decimal::ZERO),
        trade_count: Some(3),
        win_rate: Some(Decimal::new(100, 0)),
        fee_drag: Some(Decimal::ZERO),
        status: ResearchCandidateStatus::PromotedToShadowConfig,
        rejection_reason: None,
        notes: Some("scheduled shadow fixture".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        correlation_id: Some(Uuid::new_v4()),
    };
    create_research_candidate(
        pool,
        &candidate,
        None,
        ResearchCandidateDecision::PromoteToShadowConfig,
        Some("scheduled shadow fixture"),
        Some("scheduled shadow fixture"),
        &serde_json::json!({"fixture": true}),
    )
    .await
    .expect("candidate should persist");
    candidate
}

async fn seed_cross_asset_candidate_fixture(pool: &db::PgPool) -> ResearchCandidate {
    let base_open = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    seed_cross_asset_candles_through(pool, base_open, 100).await;

    let now = Utc::now();
    let config = serde_json::to_value(
        aegis_core::relative_strength_continuation_v1_default_request(now, now, None),
    )
    .expect("cross-asset config should serialize");
    let candidate = ResearchCandidate {
        id: Uuid::new_v4(),
        experiment_id: None,
        experiment_run_id: None,
        strategy_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
        symbol: "CROSS_ASSET_BASKET".to_string(),
        timeframe: "4h".to_string(),
        config,
        score: None,
        pnl_pct: None,
        max_drawdown_pct: None,
        trade_count: None,
        win_rate: None,
        fee_drag: None,
        status: ResearchCandidateStatus::Discovered,
        rejection_reason: None,
        notes: Some("cross-asset scheduled observation fixture".to_string()),
        created_at: now,
        updated_at: now,
        correlation_id: Some(Uuid::new_v4()),
    };
    create_research_candidate(
        pool,
        &candidate,
        None,
        ResearchCandidateDecision::Reopen,
        Some("cross_asset_manual_create"),
        candidate.notes.as_deref(),
        &serde_json::json!({
            "candidate_creation_mode": "cross_asset_manual_create",
            "candidate_scope": "cross_asset_research",
            "execution_authority": "NONE",
            "implementation_research_only": true
        }),
    )
    .await
    .expect("cross-asset candidate should persist");
    candidate
}

async fn seed_cross_asset_candles_through(
    pool: &db::PgPool,
    base_open: chrono::DateTime<Utc>,
    candle_count: i64,
) {
    let request =
        aegis_core::relative_strength_continuation_v1_default_request(Utc::now(), Utc::now(), None);
    let symbols = request
        .parsed_symbols()
        .expect("default cross-asset symbols should parse");
    for symbol in symbols {
        for index in 0..candle_count {
            let open_time = base_open + ChronoDuration::hours(4 * index);
            let symbol_offset = symbol
                .as_str()
                .bytes()
                .fold(0_i64, |acc, item| acc.saturating_add(i64::from(item)))
                % 1_000;
            let close = 10_000_i64 + symbol_offset + index * 10;
            upsert_candle(
                pool,
                &Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: symbol.clone(),
                    interval: CandleInterval::FourHours,
                    open_time,
                    close_time: open_time + ChronoDuration::hours(4),
                    open: Decimal::new(close - 5, 0),
                    high: Decimal::new(close + 10, 0),
                    low: Decimal::new(close - 15, 0),
                    close: Decimal::new(close, 0),
                    volume: Decimal::new(100 + index, 0),
                    quote_volume: Some(Decimal::new(close * (100 + index), 0)),
                    trade_count: 10,
                    is_closed: true,
                    created_at: open_time + ChronoDuration::hours(4),
                    updated_at: open_time + ChronoDuration::hours(4),
                },
            )
            .await
            .expect("cross-asset candle should persist");
        }
    }
}

async fn append_cross_asset_fixture_candle(pool: &db::PgPool) {
    let next_open = Utc.with_ymd_and_hms(2026, 1, 17, 16, 0, 0).unwrap();
    seed_cross_asset_candles_through(pool, next_open, 1).await;
}

fn sample_market_tick(price: Decimal, received_at: chrono::DateTime<Utc>) -> MarketTick {
    MarketTick {
        id: Uuid::new_v4(),
        exchange: MarketDataSource::Binance,
        symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
        price,
        quantity: Decimal::ONE,
        trade_time: received_at,
        received_at,
        raw_payload: None,
    }
}

fn runner_config_input(enabled: bool) -> TestnetShadowRunnerConfigInput {
    TestnetShadowRunnerConfigInput {
        enabled,
        interval_seconds: 60,
        strategies: vec!["momentum_v1".to_string()],
        symbols: vec!["BTCUSDT".to_string()],
        timeframe: "1m".to_string(),
        max_runs_per_tick: 1,
        stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy::Skip,
        notes: Some("integration".to_string()),
    }
}

async fn count_rows(pool: &db::PgPool, table: &str) -> i64 {
    let query = format!("SELECT COUNT(*) AS count FROM {table}");
    sqlx::query(&query)
        .fetch_one(pool)
        .await
        .expect("count query should succeed")
        .get::<i64, _>("count")
}

async fn seed_open_position(
    pool: &db::PgPool,
    correlation_id: Uuid,
) -> (db::PaperAccountRecord, db::PaperPositionRecord) {
    seed_pipeline_happy_path(pool).await;
    let state = app_state(pool.clone());

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

    let account = get_default_paper_account(pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    let positions = list_open_paper_positions(pool, account.id)
        .await
        .expect("paper positions should list");
    assert_eq!(positions.len(), 1);

    (
        account,
        positions.into_iter().next().expect("position should exist"),
    )
}

async fn count_position_fills(pool: &db::PgPool, position_id: Uuid) -> i64 {
    sqlx::query("SELECT COUNT(*) AS count FROM paper_fills WHERE position_id = $1")
        .bind(position_id)
        .fetch_one(pool)
        .await
        .expect("paper fill count query should succeed")
        .get::<i64, _>("count")
}

async fn count_position_journal_events(
    pool: &db::PgPool,
    position_id: Uuid,
    event_type: &str,
) -> i64 {
    sqlx::query(
        "SELECT COUNT(*) AS count FROM paper_trade_journal WHERE position_id = $1 AND event_type = $2",
    )
    .bind(position_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("paper journal count query should succeed")
    .get::<i64, _>("count")
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

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn paper_close_persists_transactional_side_effects() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let (account, open_position) = seed_open_position(&test_db.pool, Uuid::from_u128(0xc01)).await;
    let mark_price = Decimal::new(104_500, 0);
    insert_market_tick(&test_db.pool, &sample_market_tick(mark_price, Utc::now()))
        .await
        .expect("market tick should persist");

    let baseline_snapshot_count = list_paper_equity_snapshots(&test_db.pool, account.id, 20)
        .await
        .expect("paper equity snapshots should list")
        .len();
    let baseline_journal_count = list_paper_trade_journal(&test_db.pool, account.id, 50)
        .await
        .expect("paper journal should list")
        .len();

    let summary = close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: open_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc02)),
            allow_stale_price: false,
        },
    )
    .await
    .expect("paper close should succeed");

    assert_eq!(summary.status, PaperCloseStatus::Closed);
    assert_eq!(summary.exit_price, mark_price);

    let closed_position = get_paper_position(&test_db.pool, account.id, open_position.id)
        .await
        .expect("paper position query should succeed")
        .expect("closed position should exist");
    let expected_realized_pnl = (mark_price - open_position.entry_price) * open_position.quantity;
    assert_eq!(closed_position.status, "closed");
    assert_eq!(closed_position.closed_at, Some(summary.closed_at));
    assert_eq!(closed_position.realized_pnl, expected_realized_pnl);
    assert_eq!(closed_position.mark_price, Some(mark_price));

    let updated_account = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    assert_eq!(updated_account.realized_pnl, expected_realized_pnl);
    assert_eq!(
        updated_account.current_equity,
        updated_account.initial_equity + expected_realized_pnl
    );
    assert_eq!(updated_account.unrealized_pnl, Decimal::ZERO);

    let equity_snapshots = list_paper_equity_snapshots(&test_db.pool, account.id, 20)
        .await
        .expect("paper equity snapshots should list");
    assert_eq!(equity_snapshots.len(), baseline_snapshot_count + 1);
    assert_eq!(equity_snapshots[0].equity, updated_account.current_equity);
    assert_eq!(equity_snapshots[0].realized_pnl, expected_realized_pnl);

    assert_eq!(
        count_position_fills(&test_db.pool, open_position.id).await,
        2
    );

    let journal = list_paper_trade_journal(&test_db.pool, account.id, 50)
        .await
        .expect("paper journal should list");
    assert!(journal.len() >= baseline_journal_count + 2);
    assert_eq!(
        count_position_journal_events(&test_db.pool, open_position.id, "paper.position.closed")
            .await,
        1
    );
    assert!(journal
        .iter()
        .any(|entry| entry.id == summary.journal_entry_id
            && entry.event_type == "paper.position.closed"));

    let close_events = list_recent_system_events(&test_db.pool, 50)
        .await
        .expect("system events should list")
        .into_iter()
        .filter(|event| event.correlation_id == summary.correlation_id)
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert!(close_events.contains(&"paper.position.closed".to_string()));
    assert!(close_events.contains(&"paper.equity.updated".to_string()));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn paper_close_wrong_confirmation_rejects_before_mutation() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let (account, open_position) = seed_open_position(&test_db.pool, Uuid::from_u128(0xc11)).await;

    let baseline_account = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    let baseline_snapshot_count = list_paper_equity_snapshots(&test_db.pool, account.id, 20)
        .await
        .expect("paper equity snapshots should list")
        .len();
    let baseline_fill_count = count_position_fills(&test_db.pool, open_position.id).await;
    let baseline_close_journal_count =
        count_position_journal_events(&test_db.pool, open_position.id, "paper.position.closed")
            .await;

    let result = close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: open_position.id,
            confirmation_text: "close BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc12)),
            allow_stale_price: false,
        },
    )
    .await;

    match result {
        Err(api::ClosePaperPositionError::Validation(
            PaperCloseValidationIssue::WrongConfirmationText,
        )) => {}
        other => panic!("expected wrong confirmation validation error, got {other:?}"),
    }

    let unchanged_position = get_paper_position(&test_db.pool, account.id, open_position.id)
        .await
        .expect("paper position query should succeed")
        .expect("paper position should exist");
    assert_eq!(unchanged_position.status, "open");
    assert_eq!(
        count_position_fills(&test_db.pool, open_position.id).await,
        baseline_fill_count
    );
    assert_eq!(
        count_position_journal_events(&test_db.pool, open_position.id, "paper.position.closed")
            .await,
        baseline_close_journal_count
    );
    assert_eq!(
        list_paper_equity_snapshots(&test_db.pool, account.id, 20)
            .await
            .expect("paper equity snapshots should list")
            .len(),
        baseline_snapshot_count
    );

    let unchanged_account = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    assert_eq!(
        unchanged_account.realized_pnl,
        baseline_account.realized_pnl
    );
    assert_eq!(
        unchanged_account.current_equity,
        baseline_account.current_equity
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn paper_close_missing_or_stale_mark_price_rejects_before_mutation() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");

    let (missing_account, missing_position) =
        seed_open_position(&test_db.pool, Uuid::from_u128(0xc21)).await;
    let missing_baseline_account = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    let missing_result = close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: missing_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc22)),
            allow_stale_price: false,
        },
    )
    .await;
    match missing_result {
        Err(api::ClosePaperPositionError::Validation(
            PaperCloseValidationIssue::MissingMarketPrice,
        )) => {}
        other => panic!("expected missing market price validation error, got {other:?}"),
    }
    assert_eq!(
        count_position_fills(&test_db.pool, missing_position.id).await,
        1
    );
    let missing_account_after = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    assert_eq!(
        missing_account_after.current_equity,
        missing_baseline_account.current_equity
    );
    assert_eq!(
        missing_account_after.realized_pnl,
        missing_baseline_account.realized_pnl
    );

    let stale_test_db = TestDatabase::setup()
        .await
        .expect("test db should reinitialize");
    let (stale_account, stale_position) =
        seed_open_position(&stale_test_db.pool, Uuid::from_u128(0xc23)).await;
    insert_market_tick(
        &stale_test_db.pool,
        &sample_market_tick(
            Decimal::new(104_500, 0),
            Utc::now() - ChronoDuration::seconds(30),
        ),
    )
    .await
    .expect("stale market tick should persist");
    let stale_baseline_account = get_default_paper_account(&stale_test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");

    let stale_result = close_paper_position(
        &stale_test_db.pool,
        &app_state(stale_test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: stale_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc24)),
            allow_stale_price: false,
        },
    )
    .await;
    match stale_result {
        Err(api::ClosePaperPositionError::Validation(
            PaperCloseValidationIssue::StaleMarketPrice,
        )) => {}
        other => panic!("expected stale market price validation error, got {other:?}"),
    }
    assert_eq!(
        count_position_fills(&stale_test_db.pool, stale_position.id).await,
        1
    );
    let stale_account_after = get_default_paper_account(&stale_test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    assert_eq!(
        stale_account_after.current_equity,
        stale_baseline_account.current_equity
    );
    assert_eq!(
        stale_account_after.realized_pnl,
        stale_baseline_account.realized_pnl
    );
    assert_eq!(
        get_paper_position(&stale_test_db.pool, stale_account.id, stale_position.id)
            .await
            .expect("paper position query should succeed")
            .expect("paper position should exist")
            .status,
        "open"
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn paper_close_is_idempotent_after_first_success() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let (account, open_position) = seed_open_position(&test_db.pool, Uuid::from_u128(0xc31)).await;
    insert_market_tick(
        &test_db.pool,
        &sample_market_tick(Decimal::new(104_500, 0), Utc::now()),
    )
    .await
    .expect("market tick should persist");

    let first = close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: open_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc32)),
            allow_stale_price: false,
        },
    )
    .await
    .expect("first close should succeed");
    let account_after_first = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    let fill_count_after_first = count_position_fills(&test_db.pool, open_position.id).await;
    let close_journal_count_after_first =
        count_position_journal_events(&test_db.pool, open_position.id, "paper.position.closed")
            .await;

    let second = close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: open_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc33)),
            allow_stale_price: false,
        },
    )
    .await
    .expect("repeated close should return persisted summary");

    assert_eq!(second.status, PaperCloseStatus::AlreadyClosed);
    assert_eq!(second.position_id, first.position_id);
    assert_eq!(second.close_fill_id, first.close_fill_id);
    assert_eq!(second.journal_entry_id, first.journal_entry_id);
    assert_eq!(
        count_position_fills(&test_db.pool, open_position.id).await,
        fill_count_after_first
    );
    assert_eq!(
        count_position_journal_events(&test_db.pool, open_position.id, "paper.position.closed")
            .await,
        close_journal_count_after_first
    );

    let account_after_second = get_default_paper_account(&test_db.pool)
        .await
        .expect("paper account query should succeed")
        .expect("default paper account should exist");
    assert_eq!(
        account_after_second.realized_pnl,
        account_after_first.realized_pnl
    );
    assert_eq!(
        account_after_second.current_equity,
        account_after_first.current_equity
    );
    assert_eq!(
        get_paper_position(&test_db.pool, account.id, open_position.id)
            .await
            .expect("paper position query should succeed")
            .expect("paper position should exist")
            .status,
        "closed"
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn paper_position_filters_reflect_closed_positions() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let (account, open_position) = seed_open_position(&test_db.pool, Uuid::from_u128(0xc41)).await;
    insert_market_tick(
        &test_db.pool,
        &sample_market_tick(Decimal::new(104_500, 0), Utc::now()),
    )
    .await
    .expect("market tick should persist");

    close_paper_position(
        &test_db.pool,
        &app_state(test_db.pool.clone()).market_config,
        &StateActor::system("integration-test"),
        PaperClosePositionRequest {
            position_id: open_position.id,
            confirmation_text: "CLOSE BTCUSDT".to_string(),
            reason: None,
            close_mode: PaperCloseMode::MarketSimulated,
            correlation_id: Some(Uuid::from_u128(0xc42)),
            allow_stale_price: false,
        },
    )
    .await
    .expect("paper close should succeed");

    let open_positions = list_paper_positions(
        &test_db.pool,
        account.id,
        PaperPositionStatusFilter::Open,
        50,
    )
    .await
    .expect("open paper positions should list");
    let closed_positions = list_paper_positions(
        &test_db.pool,
        account.id,
        PaperPositionStatusFilter::Closed,
        50,
    )
    .await
    .expect("closed paper positions should list");
    let all_positions = list_paper_positions(
        &test_db.pool,
        account.id,
        PaperPositionStatusFilter::All,
        50,
    )
    .await
    .expect("all paper positions should list");

    assert!(open_positions
        .iter()
        .all(|position| position.id != open_position.id));
    assert!(closed_positions
        .iter()
        .any(|position| position.id == open_position.id));
    assert!(all_positions
        .iter()
        .any(|position| position.id == open_position.id));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn shadow_runner_run_once_persists_shadow_runs_without_exchange_side_effects() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    seed_pipeline_happy_path(&test_db.pool).await;
    insert_market_tick(
        &test_db.pool,
        &sample_market_tick(Decimal::new(103_500, 0), Utc::now()),
    )
    .await
    .expect("market tick should persist");

    let state = app_state(test_db.pool.clone());
    persist_testnet_shadow_runner_config(&state, &runner_config_input(false), None)
        .await
        .expect("runner config should persist");

    let before_orders = count_rows(&test_db.pool, "exchange_testnet_orders").await;
    let before_lifecycle =
        count_rows(&test_db.pool, "exchange_testnet_order_lifecycle_events").await;
    let before_shadow_runs = count_rows(&test_db.pool, "testnet_shadow_runs").await;

    let tick = run_shadow_runner_tick(
        &state,
        Some(&StateActor::system("integration-test")),
        Some(Uuid::from_u128(0xd01)),
        RunnerTickMode::ManualRunOnce,
    )
    .await
    .expect("manual run once should succeed");

    assert_eq!(tick.attempted_runs, 1);
    assert!(
        count_rows(&test_db.pool, "testnet_shadow_runs").await > before_shadow_runs,
        "runner should persist at least one shadow run"
    );
    assert_eq!(
        count_rows(&test_db.pool, "exchange_testnet_orders").await,
        before_orders
    );
    assert_eq!(
        count_rows(&test_db.pool, "exchange_testnet_order_lifecycle_events").await,
        before_lifecycle
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn shadow_runner_config_and_state_persist_across_control_actions() {
    let test_db = TestDatabase::setup()
        .await
        .expect("test db should initialize");
    let state = app_state(test_db.pool.clone());
    let actor = StateActor::system("integration-test");

    let config = persist_testnet_shadow_runner_config(&state, &runner_config_input(true), None)
        .await
        .expect("runner config should persist");
    assert!(config.enabled);

    let (running_state, _) = apply_testnet_shadow_runner_control_action(
        &state,
        Some(&actor),
        TestnetShadowRunnerControlAction::Start,
        Uuid::from_u128(0xd11),
    )
    .await
    .expect("start should succeed");
    assert_eq!(running_state.status, TestnetShadowRunnerStatus::Running);

    let (paused_state, _) = apply_testnet_shadow_runner_control_action(
        &state,
        Some(&actor),
        TestnetShadowRunnerControlAction::Pause,
        Uuid::from_u128(0xd12),
    )
    .await
    .expect("pause should succeed");
    assert_eq!(paused_state.status, TestnetShadowRunnerStatus::Paused);

    let (resumed_state, _) = apply_testnet_shadow_runner_control_action(
        &state,
        Some(&actor),
        TestnetShadowRunnerControlAction::Resume,
        Uuid::from_u128(0xd13),
    )
    .await
    .expect("resume should succeed");
    assert_eq!(resumed_state.status, TestnetShadowRunnerStatus::Running);

    let snapshot = load_testnet_shadow_runner_snapshot(&state)
        .await
        .expect("runner snapshot should load");
    assert!(snapshot.config.enabled);
    assert_eq!(snapshot.state.status, TestnetShadowRunnerStatus::Running);
}
