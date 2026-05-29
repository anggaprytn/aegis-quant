use crate::{ensure_strategy_config, reason_code, AppState};
use aegis_core::{
    summarize_candle_continuity, Candle, CandleInterval, DataFreshnessStatus, EventEnvelope,
    ExchangeEnvironment, ExchangeName, ExchangeOrderSide, ExchangeOrderType,
    MarketDataQualityRequest, MarketDataQualityStatus, RiskCheckContext, RiskEvaluationDecision,
    RiskRejectionReason, Side, SignalReason, StrategyConfig, StrategyEvaluationContext, StrategyId,
    StrategySignal, Symbol, TestnetShadowDecision, TestnetShadowIntent, TestnetShadowModeConfig,
    TestnetShadowRejectionReason, TestnetShadowRunRequest, TestnetShadowRunResult,
    TestnetShadowStatus,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use db::{
    find_signal_by_identity, get_latest_market_tick, get_recent_closed_candles, get_risk_config,
    get_signal_by_id, get_system_state, insert_audit_log,
    insert_research_candidate_shadow_run_link, insert_risk_decision, insert_signal_deduped,
    insert_system_event, insert_testnet_shadow_run, list_market_feed_statuses,
    load_risk_state_snapshot, resolve_promoted_research_candidate_for_shadow_run,
    risk_config_from_record, update_strategy_state, ShadowRunCandidateMatchOutcome, StateActor,
    TestnetShadowRunRecord,
};
use risk_engine::RiskEvaluator;
use rust_decimal::Decimal;
use serde_json::json;
use strategy_engine::{evaluate as evaluate_strategy, required_candle_count};
use telemetry::telemetry;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ResolvedPrice {
    source: String,
    price: Decimal,
}

#[derive(Debug, Clone)]
struct ShadowOutcomePlan {
    decision: TestnetShadowDecision,
    status: TestnetShadowStatus,
    signal_id: Option<Uuid>,
    risk_decision_id: Option<Uuid>,
    would_submit_order: Option<TestnetShadowIntent>,
    reasons: Vec<TestnetShadowRejectionReason>,
    price_source: Option<String>,
    resolved_price: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct ShadowEvaluationInput {
    kill_switch_active: bool,
    strategy_enabled: bool,
    required_feed_block: Option<TestnetShadowRejectionReason>,
    feed_warning: Option<TestnetShadowRejectionReason>,
    candle_block: Option<TestnetShadowRejectionReason>,
    timeframe_matches: bool,
    signal: Option<StrategySignal>,
    signal_reason: SignalReason,
    risk_decision_id: Option<Uuid>,
    risk_decision: Option<RiskEvaluationDecision>,
    risk_reasons: Vec<RiskRejectionReason>,
    price: Option<ResolvedPrice>,
    approved_notional: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct ShadowCandleReadiness {
    candles: Vec<Candle>,
    block_reason: Option<TestnetShadowRejectionReason>,
}

#[derive(Debug)]
pub enum TestnetShadowRunApiError {
    Validation {
        code: &'static str,
        message: String,
        reason: &'static str,
        candidate_linking_result: &'static str,
    },
    Conflict {
        code: &'static str,
        message: String,
        reason: &'static str,
        candidate_linking_result: &'static str,
    },
}

impl TestnetShadowRunApiError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Validation { code, .. } | Self::Conflict { code, .. } => code,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Validation { message, .. } | Self::Conflict { message, .. } => message,
        }
    }

    pub fn reason(&self) -> &'static str {
        match self {
            Self::Validation { reason, .. } | Self::Conflict { reason, .. } => reason,
        }
    }

    pub fn candidate_linking_result(&self) -> &'static str {
        match self {
            Self::Validation {
                candidate_linking_result,
                ..
            }
            | Self::Conflict {
                candidate_linking_result,
                ..
            } => candidate_linking_result,
        }
    }

    pub fn invalid_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::Validation {
            code,
            message: message.into(),
            reason: "invalid_request",
            candidate_linking_result: "not_evaluated",
        }
    }

    pub fn signal_conflict(candidate_linking_result: &'static str) -> Self {
        Self::Conflict {
            code: "shadow_signal_conflict",
            message: "A duplicate-like signal persistence race prevented this shadow run from being recorded. Retry the request.".to_string(),
            reason: "signal_reference_missing",
            candidate_linking_result,
        }
    }
}

impl std::fmt::Display for TestnetShadowRunApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for TestnetShadowRunApiError {}

pub async fn run_testnet_shadow_once(
    state: &AppState,
    actor: Option<&StateActor>,
    request: TestnetShadowRunRequest,
) -> Result<TestnetShadowRunResult> {
    let strategy_id = request.strategy_id.parse().map_err(|_| {
        TestnetShadowRunApiError::invalid_request(
            "invalid_strategy_id",
            "strategy_id is invalid for shadow execution.",
        )
    })?;
    let symbol = Symbol::new(request.symbol.clone()).map_err(|_| {
        TestnetShadowRunApiError::invalid_request(
            "invalid_symbol",
            "symbol is invalid for shadow execution.",
        )
    })?;
    let timeframe: CandleInterval = request.timeframe.parse().map_err(|_| {
        TestnetShadowRunApiError::invalid_request(
            "invalid_timeframe",
            "timeframe is invalid for shadow execution.",
        )
    })?;
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let created_at = Utc::now();

    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.shadow.started",
            correlation_id,
            &state.config.app_name,
            json!({
                "strategy_id": request.strategy_id,
                "symbol": request.symbol,
                "timeframe": request.timeframe,
            }),
        ),
    )
    .await;

    let system_state = get_system_state(&state.db_pool)
        .await
        .context("failed to load system state")?;
    let config = ensure_strategy_config(state, strategy_id)
        .await
        .context("failed to load strategy config")?;
    let risk_config = get_risk_config(&state.db_pool)
        .await
        .context("failed to load persisted risk config")?
        .map(|record| risk_config_from_record(&record))
        .transpose()
        .context("persisted risk config is invalid")?
        .unwrap_or_default();
    let mode_config = TestnetShadowModeConfig {
        stale_price_threshold_seconds: shadow_price_threshold_seconds(&config, &risk_config),
    };
    let feed_status_block = shadow_feed_block_reason(state, &symbol).await?;
    let (required_feed_block, feed_warning) = if strategy_requires_live_market_feed(strategy_id) {
        (feed_status_block, None)
    } else {
        (None, feed_status_block.map(optional_market_feed_warning))
    };
    let candle_readiness =
        load_shadow_candle_readiness(state, &symbol, timeframe, &config, created_at).await?;

    let mut signal = None;
    let mut signal_reason = SignalReason::ConditionsNotMet;
    let mut risk_decision_id = None;
    let mut risk_decision = None;
    let mut risk_reasons = Vec::new();
    let mut approved_notional = None;
    let mut price = None;

    if !system_state.kill_switch_enabled
        && config.enabled
        && config.timeframe == timeframe
        && required_feed_block.is_none()
        && candle_readiness.block_reason.is_none()
    {
        let evaluation = evaluate_strategy(StrategyEvaluationContext {
            correlation_id,
            strategy_id,
            symbol: symbol.clone(),
            config: config.clone(),
            candles: candle_readiness.candles.clone(),
            evaluated_at: created_at,
        })
        .context("failed to evaluate strategy")?;
        signal_reason = evaluation.reason;

        if let Some(generated_signal) = evaluation.signal.clone() {
            let signal_outcome = insert_signal_deduped(&state.db_pool, &generated_signal)
                .await
                .context("failed to persist signal")?;
            update_strategy_state(
                &state.db_pool,
                strategy_id,
                evaluation.evaluated_at,
                evaluation.reason,
                Some(signal_outcome.signal.id),
                Some(signal_outcome.signal.created_at),
            )
            .await
            .context("failed to update strategy state after signal generation")?;
            signal = Some(generated_signal);
            let risk_context = RiskCheckContext {
                signal_id: signal_outcome.signal.id,
                correlation_id,
                strategy_id: signal_outcome.signal.strategy_id.clone(),
                symbol: symbol.clone(),
                side: parse_signal_side(&signal_outcome.signal.side)?,
                suggested_notional: signal_outcome.signal.suggested_notional,
                signal_created_at: signal_outcome.signal.created_at,
                evaluated_at: created_at,
            };
            let snapshot = load_risk_state_snapshot(&state.db_pool)
                .await
                .context("failed to load risk state snapshot")?;
            let evaluator = RiskEvaluator::new(risk_config);
            let risk_evaluation = evaluator.evaluate(&risk_context, &snapshot);
            let persisted_risk = insert_risk_decision(
                &state.db_pool,
                &state.config.app_name,
                &risk_context,
                &risk_evaluation,
            )
            .await
            .context("failed to persist risk decision")?;

            risk_decision_id = Some(persisted_risk.risk_decision_id);
            risk_decision = Some(risk_evaluation.decision);
            risk_reasons = risk_evaluation.reasons.clone();
            approved_notional = risk_evaluation.approved_notional;

            let decision_label = match risk_evaluation.decision {
                RiskEvaluationDecision::Approved => "approved",
                RiskEvaluationDecision::Rejected => "rejected",
            };
            telemetry().inc_risk_decision(
                decision_label,
                risk_reasons
                    .first()
                    .map(|reason| reason_code(*reason))
                    .unwrap_or("none"),
            );

            if risk_evaluation.decision == RiskEvaluationDecision::Approved {
                price = resolve_shadow_price(state, &symbol, timeframe, created_at, &mode_config)
                    .await?;
            }
        } else {
            update_strategy_state(
                &state.db_pool,
                strategy_id,
                evaluation.evaluated_at,
                evaluation.reason,
                None,
                None,
            )
            .await
            .context("failed to update strategy state after no-signal evaluation")?;
        }
    }

    let shadow_signal = signal.clone();
    let plan = evaluate_shadow_outcome(ShadowEvaluationInput {
        kill_switch_active: system_state.kill_switch_enabled,
        strategy_enabled: config.enabled,
        required_feed_block,
        feed_warning,
        candle_block: candle_readiness.block_reason,
        timeframe_matches: config.timeframe == timeframe,
        signal,
        signal_reason,
        risk_decision_id,
        risk_decision,
        risk_reasons,
        price,
        approved_notional: approved_notional.or(Some(config.suggested_notional)),
    })?;

    let result = persist_shadow_result(
        state,
        actor,
        &request,
        correlation_id,
        created_at,
        plan,
        shadow_signal.as_ref(),
    )
    .await?;

    Ok(result)
}

fn strategy_requires_live_market_feed(_strategy_id: StrategyId) -> bool {
    false
}

fn shadow_price_threshold_seconds(
    config: &StrategyConfig,
    risk_config: &aegis_core::RiskConfig,
) -> u32 {
    let strategy_threshold_seconds = config
        .max_signal_age_ms
        .checked_div(1_000)
        .unwrap_or(0)
        .max(1);
    i64::from(risk_config.stale_feed_threshold_seconds)
        .max(strategy_threshold_seconds)
        .try_into()
        .unwrap_or(u32::MAX)
}

fn optional_market_feed_warning(
    reason: TestnetShadowRejectionReason,
) -> TestnetShadowRejectionReason {
    match reason {
        TestnetShadowRejectionReason::StaleFeed
        | TestnetShadowRejectionReason::MarketFeedDegraded => {
            TestnetShadowRejectionReason::MarketFeedStaleWarning
        }
        other => other,
    }
}

async fn load_shadow_candle_readiness(
    state: &AppState,
    symbol: &Symbol,
    timeframe: CandleInterval,
    config: &StrategyConfig,
    evaluated_at: chrono::DateTime<Utc>,
) -> Result<ShadowCandleReadiness> {
    let required_candles = required_candle_count(config);
    let candles = get_recent_closed_candles(&state.db_pool, symbol, timeframe, required_candles)
        .await
        .context("failed to query closed candles")?;

    let block_reason = shadow_candle_block_reason(
        state.market_config.exchange,
        symbol,
        timeframe,
        config,
        evaluated_at,
        &candles,
    )?;

    Ok(ShadowCandleReadiness {
        candles,
        block_reason,
    })
}

fn shadow_candle_block_reason(
    exchange: aegis_core::MarketDataSource,
    symbol: &Symbol,
    timeframe: CandleInterval,
    config: &StrategyConfig,
    evaluated_at: chrono::DateTime<Utc>,
    candles: &[Candle],
) -> Result<Option<TestnetShadowRejectionReason>> {
    let required_candles = required_candle_count(config);
    if i64::try_from(candles.len()).unwrap_or(i64::MAX) < required_candles {
        return Ok(Some(TestnetShadowRejectionReason::InsufficientHistory));
    }

    let Some(first) = candles.first() else {
        return Ok(Some(TestnetShadowRejectionReason::InsufficientHistory));
    };
    let Some(latest) = candles.last() else {
        return Ok(Some(TestnetShadowRejectionReason::InsufficientHistory));
    };

    let max_age_ms = shadow_candle_max_age_ms(config, timeframe);
    let latest_age_ms = evaluated_at
        .signed_duration_since(latest.close_time)
        .num_milliseconds();
    if latest_age_ms > max_age_ms {
        return Ok(Some(TestnetShadowRejectionReason::CandleDataStale));
    }

    let quality_request = MarketDataQualityRequest {
        exchange,
        symbol: symbol.as_str().to_string(),
        interval: timeframe.as_str().to_string(),
        start_time: first.open_time,
        end_time: latest.open_time + timeframe.duration(),
        expected_interval_seconds: Some(timeframe.duration().num_seconds()),
        max_allowed_gap_count: Some(0),
        max_allowed_gap_pct: Some(Decimal::ZERO),
    };
    let quality = summarize_candle_continuity(&quality_request, candles, 5)
        .context("failed to summarize shadow candle continuity")?;
    if matches!(
        quality.status,
        MarketDataQualityStatus::Bad
            | MarketDataQualityStatus::Degraded
            | MarketDataQualityStatus::InsufficientData
    ) {
        return Ok(Some(TestnetShadowRejectionReason::DataStale));
    }

    Ok(None)
}

fn shadow_candle_max_age_ms(config: &StrategyConfig, timeframe: CandleInterval) -> i64 {
    config
        .max_signal_age_ms
        .max(timeframe.duration().num_milliseconds())
}

async fn persist_shadow_result(
    state: &AppState,
    actor: Option<&StateActor>,
    request: &TestnetShadowRunRequest,
    correlation_id: Uuid,
    created_at: chrono::DateTime<Utc>,
    plan: ShadowOutcomePlan,
    signal: Option<&StrategySignal>,
) -> Result<TestnetShadowRunResult> {
    let candidate_match = resolve_promoted_research_candidate_for_shadow_run(
        &state.db_pool,
        &request.strategy_id,
        &request.symbol,
        &request.timeframe,
    )
    .await
    .context("failed to resolve promoted research candidate shadow link")?;
    let candidate_linking_result = candidate_match_outcome_label(candidate_match);
    let signal_id = stabilize_shadow_signal_id(
        state,
        request,
        plan.signal_id,
        signal,
        candidate_linking_result,
        correlation_id,
    )
    .await?;
    let run_record = TestnetShadowRunRecord {
        id: Uuid::new_v4(),
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        decision: plan.decision.as_str().to_string(),
        signal_id,
        risk_decision_id: plan.risk_decision_id,
        would_submit_payload: plan
            .would_submit_order
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
        price_source: plan.price_source.clone(),
        resolved_price: plan.resolved_price,
        reasons: plan
            .reasons
            .iter()
            .map(|value| value.as_str().to_string())
            .collect(),
        status: plan.status.as_str().to_string(),
        created_at,
        correlation_id: Some(correlation_id),
    };
    let persisted = match insert_testnet_shadow_run(&state.db_pool, &run_record).await {
        Ok(persisted) => persisted,
        Err(err) if is_missing_shadow_signal_fk(&err) => {
            return Err(TestnetShadowRunApiError::signal_conflict(candidate_linking_result).into())
        }
        Err(err) => return Err(err).context("failed to persist testnet shadow run"),
    };
    let result = db::testnet_shadow_run_result_from_record(&persisted)?;

    if let ShadowRunCandidateMatchOutcome::Matched(candidate_id) = candidate_match {
        if insert_research_candidate_shadow_run_link(
            &state.db_pool,
            candidate_id,
            persisted.id,
            created_at,
        )
        .await
        .context("failed to persist research candidate shadow run link")?
        .is_some()
        {
            let _ = insert_system_event(
                &state.db_pool,
                &EventEnvelope::new(
                    "research.candidate.shadow_run_linked",
                    correlation_id,
                    &state.config.app_name,
                    json!({
                        "candidate_id": candidate_id,
                        "shadow_run_id": persisted.id,
                        "strategy_id": result.strategy_id,
                        "symbol": result.symbol,
                        "timeframe": result.timeframe,
                    }),
                ),
            )
            .await;
        }
    }

    telemetry().inc_exchange_testnet_shadow_run(
        &result.strategy_id,
        &result.symbol,
        result.decision.as_str(),
    );
    if result.decision == TestnetShadowDecision::WouldSubmit {
        telemetry().inc_exchange_testnet_shadow_would_submit(&result.strategy_id, &result.symbol);
    } else {
        for reason in &result.reasons {
            telemetry().inc_exchange_testnet_shadow_rejection(reason.as_str());
        }
    }

    let event_type = match result.decision {
        TestnetShadowDecision::WouldSubmit | TestnetShadowDecision::NoSignal => {
            "exchange.testnet.shadow.completed"
        }
        _ => "exchange.testnet.shadow.rejected",
    };
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            correlation_id,
            &state.config.app_name,
            json!({
                "run_id": result.run_id,
                "strategy_id": result.strategy_id,
                "symbol": result.symbol,
                "timeframe": result.timeframe,
                "decision": result.decision.as_str(),
                "reasons": result.reasons.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
            }),
        ),
    )
    .await;

    if let Some(actor) = actor {
        let _ = insert_audit_log(
            &state.db_pool,
            correlation_id,
            actor,
            "exchange.testnet.shadow.run",
            &result.symbol,
            &json!({
                "run_id": result.run_id,
                "strategy_id": result.strategy_id,
                "timeframe": result.timeframe,
                "decision": result.decision.as_str(),
                "signal_id": result.signal_id,
                "risk_decision_id": result.risk_decision_id,
            }),
        )
        .await;
    }

    Ok(result)
}

async fn stabilize_shadow_signal_id(
    state: &AppState,
    request: &TestnetShadowRunRequest,
    signal_id: Option<Uuid>,
    signal: Option<&StrategySignal>,
    candidate_linking_result: &'static str,
    correlation_id: Uuid,
) -> Result<Option<Uuid>> {
    let Some(signal_id) = signal_id else {
        return Ok(None);
    };

    if get_signal_by_id(&state.db_pool, signal_id)
        .await
        .context("failed to verify persisted signal before shadow run insert")?
        .is_some()
    {
        return Ok(Some(signal_id));
    }

    let Some(signal) = signal else {
        return Err(TestnetShadowRunApiError::signal_conflict(candidate_linking_result).into());
    };

    let repaired = find_signal_by_identity(
        &state.db_pool,
        signal.strategy_id.as_str(),
        signal.symbol.as_str(),
        signal.timeframe.as_str(),
        signal.side.as_str(),
        signal.reason.as_str(),
        signal.source_candle_open_time,
    )
    .await
    .context("failed to repair missing persisted signal before shadow run insert")?;

    if let Some(repaired) = repaired {
        warn!(
            correlation_id = %correlation_id,
            strategy_id = %request.strategy_id,
            symbol = %request.symbol,
            timeframe = %request.timeframe,
            candidate_linking_result,
            reason = "signal_reference_repaired",
            missing_signal_id = %signal_id,
            repaired_signal_id = %repaired.id,
            "repaired missing persisted signal reference before shadow run insert"
        );
        return Ok(Some(repaired.id));
    }

    Err(TestnetShadowRunApiError::signal_conflict(candidate_linking_result).into())
}

fn candidate_match_outcome_label(outcome: ShadowRunCandidateMatchOutcome) -> &'static str {
    match outcome {
        ShadowRunCandidateMatchOutcome::NotFound => "not_found",
        ShadowRunCandidateMatchOutcome::Matched(_) => "matched",
        ShadowRunCandidateMatchOutcome::Ambiguous => "ambiguous",
    }
}

fn is_missing_shadow_signal_fk(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>() else {
            return false;
        };
        match sqlx_err {
            sqlx::Error::Database(db_err) => {
                db_err.is_foreign_key_violation()
                    && db_err.constraint() == Some("testnet_shadow_runs_signal_id_fkey")
            }
            _ => false,
        }
    })
}

async fn shadow_feed_block_reason(
    state: &AppState,
    symbol: &Symbol,
) -> Result<Option<TestnetShadowRejectionReason>> {
    let feed = list_market_feed_statuses(&state.db_pool)
        .await
        .context("failed to query market feed status")?
        .into_iter()
        .find(|feed| {
            feed.exchange == state.market_config.exchange.as_str() && feed.symbol == symbol.as_str()
        });

    let Some(feed) = feed else {
        return Ok(Some(TestnetShadowRejectionReason::MarketFeedUnavailable));
    };

    if feed.freshness_status != DataFreshnessStatus::Fresh {
        return Ok(Some(TestnetShadowRejectionReason::StaleFeed));
    }

    if feed.status != "connected" {
        return Ok(Some(TestnetShadowRejectionReason::MarketFeedDegraded));
    }

    Ok(None)
}

async fn resolve_shadow_price(
    state: &AppState,
    symbol: &Symbol,
    timeframe: CandleInterval,
    now: chrono::DateTime<Utc>,
    mode_config: &TestnetShadowModeConfig,
) -> Result<Option<ResolvedPrice>> {
    let stale_threshold =
        chrono::Duration::seconds(i64::from(mode_config.stale_price_threshold_seconds));
    let latest_tick = get_latest_market_tick(&state.db_pool, state.market_config.exchange, symbol)
        .await
        .context("failed to load latest market tick")?;

    if let Some(tick) = latest_tick {
        if tick.price > Decimal::ZERO
            && now.signed_duration_since(tick.received_at) <= stale_threshold
        {
            return Ok(Some(ResolvedPrice {
                source: "market_tick".to_string(),
                price: tick.price,
            }));
        }
    }

    let latest_candle = get_recent_closed_candles(&state.db_pool, symbol, timeframe, 1)
        .await
        .context("failed to load latest closed candle")?
        .into_iter()
        .last();

    let Some(candle) = latest_candle else {
        return Ok(None);
    };

    if candle.close <= Decimal::ZERO
        || now.signed_duration_since(candle.close_time) > stale_threshold
    {
        return Ok(None);
    }

    Ok(Some(ResolvedPrice {
        source: "closed_candle".to_string(),
        price: candle.close,
    }))
}

fn evaluate_shadow_outcome(input: ShadowEvaluationInput) -> Result<ShadowOutcomePlan> {
    if input.kill_switch_active {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::SkippedKillSwitch,
            status: TestnetShadowStatus::Rejected,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons: vec![TestnetShadowRejectionReason::KillSwitchActive],
            price_source: None,
            resolved_price: None,
        });
    }

    if !input.strategy_enabled {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::SkippedDisabledStrategy,
            status: TestnetShadowStatus::Rejected,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons: vec![TestnetShadowRejectionReason::StrategyDisabled],
            price_source: None,
            resolved_price: None,
        });
    }

    if !input.timeframe_matches {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::Error,
            status: TestnetShadowStatus::Error,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons: vec![TestnetShadowRejectionReason::UnsupportedTimeframe],
            price_source: None,
            resolved_price: None,
        });
    }

    if let Some(reason) = input.required_feed_block {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::SkippedStaleFeed,
            status: TestnetShadowStatus::Rejected,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons: vec![reason],
            price_source: None,
            resolved_price: None,
        });
    }

    if let Some(reason) = input.candle_block {
        return Ok(ShadowOutcomePlan {
            decision: match reason {
                TestnetShadowRejectionReason::CandleDataStale
                | TestnetShadowRejectionReason::DataStale
                | TestnetShadowRejectionReason::InsufficientHistory => {
                    TestnetShadowDecision::CandleDataStale
                }
                _ => TestnetShadowDecision::Error,
            },
            status: TestnetShadowStatus::Rejected,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons: vec![reason],
            price_source: None,
            resolved_price: None,
        });
    }

    let Some(signal) = input.signal else {
        let mut reasons = signal_reason_to_shadow_reasons(input.signal_reason);
        if let Some(warning) = input.feed_warning {
            reasons.push(warning);
        }
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::NoSignal,
            status: TestnetShadowStatus::Completed,
            signal_id: None,
            risk_decision_id: None,
            would_submit_order: None,
            reasons,
            price_source: None,
            resolved_price: None,
        });
    };

    if input.risk_decision != Some(RiskEvaluationDecision::Approved) {
        let reasons = if input.risk_reasons.is_empty() {
            vec![TestnetShadowRejectionReason::RiskRejected]
        } else {
            input
                .risk_reasons
                .iter()
                .map(|reason| risk_reason_to_shadow_reason(*reason))
                .collect()
        };
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::RiskRejected,
            status: TestnetShadowStatus::Rejected,
            signal_id: Some(signal.signal_id),
            risk_decision_id: input.risk_decision_id,
            would_submit_order: None,
            reasons,
            price_source: None,
            resolved_price: None,
        });
    }

    let Some(price) = input.price else {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::SkippedStalePrice,
            status: TestnetShadowStatus::Rejected,
            signal_id: Some(signal.signal_id),
            risk_decision_id: input.risk_decision_id,
            would_submit_order: None,
            reasons: vec![TestnetShadowRejectionReason::StalePrice],
            price_source: None,
            resolved_price: None,
        });
    };

    let quote_notional = input
        .approved_notional
        .ok_or_else(|| anyhow!("approved notional is required for WOULD_SUBMIT"))?;
    let quantity = (quote_notional / price.price).round_dp(8);
    if quantity <= Decimal::ZERO {
        return Ok(ShadowOutcomePlan {
            decision: TestnetShadowDecision::SkippedStalePrice,
            status: TestnetShadowStatus::Rejected,
            signal_id: Some(signal.signal_id),
            risk_decision_id: input.risk_decision_id,
            would_submit_order: None,
            reasons: vec![TestnetShadowRejectionReason::InvalidPrice],
            price_source: Some(price.source),
            resolved_price: Some(price.price),
        });
    }

    let would_submit_order = TestnetShadowIntent {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: signal.symbol,
        side: exchange_order_side_from_signal(signal.side),
        order_type: ExchangeOrderType::Market,
        time_in_force: None,
        quantity: Some(quantity),
        quote_notional: Some(quote_notional),
        limit_price: None,
        risk_decision_id: input.risk_decision_id,
    };
    let mut reasons = Vec::new();
    if let Some(warning) = input.feed_warning {
        reasons.push(warning);
    }

    Ok(ShadowOutcomePlan {
        decision: TestnetShadowDecision::WouldSubmit,
        status: TestnetShadowStatus::Completed,
        signal_id: Some(signal.signal_id),
        risk_decision_id: input.risk_decision_id,
        would_submit_order: Some(would_submit_order),
        reasons,
        price_source: Some(price.source),
        resolved_price: Some(price.price),
    })
}

fn signal_reason_to_shadow_reasons(reason: SignalReason) -> Vec<TestnetShadowRejectionReason> {
    match reason {
        SignalReason::ConditionsNotMet => vec![TestnetShadowRejectionReason::ConditionsNotMet],
        SignalReason::InsufficientHistory => {
            vec![TestnetShadowRejectionReason::InsufficientHistory]
        }
        SignalReason::StrategyDisabled => vec![TestnetShadowRejectionReason::StrategyDisabled],
        _ => vec![TestnetShadowRejectionReason::NoSignal],
    }
}

fn risk_reason_to_shadow_reason(reason: RiskRejectionReason) -> TestnetShadowRejectionReason {
    match reason_code(reason) {
        "kill_switch_active" => TestnetShadowRejectionReason::KillSwitchActive,
        "max_open_positions_exceeded" => TestnetShadowRejectionReason::MaxOpenPositionsExceeded,
        "max_daily_loss_exceeded" => TestnetShadowRejectionReason::MaxDailyLossExceeded,
        "max_weekly_loss_exceeded" => TestnetShadowRejectionReason::MaxWeeklyLossExceeded,
        "max_consecutive_losses_exceeded" => {
            TestnetShadowRejectionReason::MaxConsecutiveLossesExceeded
        }
        "signal_too_old" => TestnetShadowRejectionReason::SignalTooOld,
        "duplicate_order_detected" => TestnetShadowRejectionReason::DuplicateOrderDetected,
        "data_stale" => TestnetShadowRejectionReason::DataStale,
        "position_notional_exceeded" => TestnetShadowRejectionReason::PositionNotionalExceeded,
        "cooldown_active" => TestnetShadowRejectionReason::CooldownActive,
        _ => TestnetShadowRejectionReason::UnsupportedState,
    }
}

fn parse_signal_side(value: &str) -> Result<Side> {
    let parsed = value
        .parse::<aegis_core::SignalSide>()
        .context("invalid persisted signal side")?;
    Ok(parsed.into())
}

fn exchange_order_side_from_signal(side: aegis_core::SignalSide) -> ExchangeOrderSide {
    match side {
        aegis_core::SignalSide::Buy => ExchangeOrderSide::Buy,
        aegis_core::SignalSide::Sell => ExchangeOrderSide::Sell,
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_shadow_outcome, ResolvedPrice, ShadowEvaluationInput};
    use aegis_core::{
        CandleInterval, ExchangeEnvironment, RiskEvaluationDecision, RiskRejectionReason,
        SignalConfidence, SignalReason, SignalSide, StrategyId, StrategySignal, Symbol,
        TestnetShadowDecision,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_signal() -> StrategySignal {
        let created_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        StrategySignal {
            signal_id: Uuid::from_u128(0x111),
            strategy_id: StrategyId::MomentumV1,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            side: SignalSide::Buy,
            confidence: SignalConfidence::new(Decimal::new(80, 2)).unwrap(),
            timeframe: CandleInterval::OneMinute,
            reason: SignalReason::ThreeConsecutiveHigherCloses,
            suggested_notional: Decimal::new(100_000, 0),
            stop_loss_pct: None,
            take_profit_pct: None,
            source_candle_open_time: created_at,
            correlation_id: Uuid::from_u128(0x222),
            created_at,
        }
    }

    fn base_input() -> ShadowEvaluationInput {
        ShadowEvaluationInput {
            kill_switch_active: false,
            strategy_enabled: true,
            required_feed_block: None,
            feed_warning: None,
            candle_block: None,
            timeframe_matches: true,
            signal: Some(sample_signal()),
            signal_reason: SignalReason::ThreeConsecutiveHigherCloses,
            risk_decision_id: Some(Uuid::from_u128(0x333)),
            risk_decision: Some(RiskEvaluationDecision::Approved),
            risk_reasons: Vec::new(),
            price: Some(ResolvedPrice {
                source: "market_tick".to_string(),
                price: Decimal::new(100_000, 0),
            }),
            approved_notional: Some(Decimal::new(100_000, 0)),
        }
    }

    #[test]
    fn kill_switch_returns_skipped_kill_switch() {
        let mut input = base_input();
        input.kill_switch_active = true;
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::SkippedKillSwitch);
    }

    #[test]
    fn disabled_strategy_returns_skipped_disabled_strategy() {
        let mut input = base_input();
        input.strategy_enabled = false;
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(
            result.decision,
            TestnetShadowDecision::SkippedDisabledStrategy
        );
    }

    #[test]
    fn no_signal_returns_no_signal() {
        let mut input = base_input();
        input.signal = None;
        input.signal_reason = SignalReason::ConditionsNotMet;
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::NoSignal);
    }

    #[test]
    fn candle_only_missing_market_feed_warning_still_allows_no_signal() {
        let mut input = base_input();
        input.signal = None;
        input.signal_reason = SignalReason::ConditionsNotMet;
        input.feed_warning = Some(aegis_core::TestnetShadowRejectionReason::MarketFeedUnavailable);
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::NoSignal);
        assert!(result
            .reasons
            .contains(&aegis_core::TestnetShadowRejectionReason::MarketFeedUnavailable));
    }

    #[test]
    fn candle_only_missing_market_feed_warning_still_allows_would_submit() {
        let mut input = base_input();
        input.feed_warning = Some(aegis_core::TestnetShadowRejectionReason::MarketFeedUnavailable);
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::WouldSubmit);
        assert!(result.would_submit_order.is_some());
        assert!(result
            .reasons
            .contains(&aegis_core::TestnetShadowRejectionReason::MarketFeedUnavailable));
    }

    #[test]
    fn stale_candle_data_returns_candle_data_stale() {
        let mut input = base_input();
        input.candle_block = Some(aegis_core::TestnetShadowRejectionReason::CandleDataStale);
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::CandleDataStale);
        assert_eq!(
            result.reasons,
            vec![aegis_core::TestnetShadowRejectionReason::CandleDataStale]
        );
    }

    #[test]
    fn required_feed_block_still_blocks_feed_dependent_path() {
        let mut input = base_input();
        input.required_feed_block = Some(aegis_core::TestnetShadowRejectionReason::StaleFeed);
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::SkippedStaleFeed);
        assert_eq!(
            result.reasons,
            vec![aegis_core::TestnetShadowRejectionReason::StaleFeed]
        );
    }

    #[test]
    fn risk_rejected_returns_risk_rejected() {
        let mut input = base_input();
        input.risk_decision = Some(RiskEvaluationDecision::Rejected);
        input.risk_reasons = vec![RiskRejectionReason::MaxDailyLossExceeded];
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::RiskRejected);
    }

    #[test]
    fn stale_or_missing_price_blocks_would_submit() {
        let mut input = base_input();
        input.price = None;
        let result = evaluate_shadow_outcome(input).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::SkippedStalePrice);
    }

    #[test]
    fn approved_risk_and_fresh_price_returns_would_submit() {
        let result = evaluate_shadow_outcome(base_input()).unwrap();
        assert_eq!(result.decision, TestnetShadowDecision::WouldSubmit);
    }

    #[test]
    fn would_submit_never_uses_live_environment() {
        let result = evaluate_shadow_outcome(base_input()).unwrap();
        assert_eq!(
            result
                .would_submit_order
                .expect("would submit order expected")
                .environment,
            ExchangeEnvironment::Testnet
        );
    }
}
