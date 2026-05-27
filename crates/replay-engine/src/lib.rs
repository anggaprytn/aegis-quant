use aegis_core::{
    BacktestEquityPoint, BacktestPosition, BacktestRequest, BacktestResult, BacktestTrade, Candle,
    CandleInterval, EventEnvelope, ReplayRunStatus, Side, StrategyConfig,
    StrategyConfigUpdateRequest, StrategyEvaluationContext, StrategyExperimentCandidate,
    StrategyExperimentComparison, StrategyExperimentGlobalRanking,
    StrategyExperimentGlobalRankingEntry, StrategyExperimentMetric, StrategyExperimentRequest,
    StrategyExperimentResult, StrategyExperimentRun, StrategyExperimentStatus, StrategyId,
    StrategyMultiTimeframeExperimentRequest, StrategyMultiTimeframeExperimentResult,
    StrategyTimeframeCandidate, StrategyTimeframeComparison, StrategyWalkForwardCandidate,
    StrategyWalkForwardRecommendation, StrategyWalkForwardRequest, StrategyWalkForwardResult,
    StrategyWalkForwardRobustnessStatus, StrategyWalkForwardRobustnessSummary,
    StrategyWalkForwardStatus, StrategyWalkForwardWindow, StrategyWalkForwardWindowResult, Symbol,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use db::{
    backtest_result_from_record, get_backtest_run, get_closed_candles_range,
    get_strategy_experiment_run, get_strategy_status, insert_backtest_equity_points,
    insert_backtest_run, insert_backtest_trade, insert_strategy_experiment,
    insert_strategy_experiment_runs, insert_strategy_walk_forward_run,
    insert_strategy_walk_forward_windows, insert_system_event, strategy_config_from_record,
    update_backtest_run_completed, PgPool,
};
use rust_decimal::Decimal;
use serde_json::json;
use strategy_engine::{
    evaluate as evaluate_strategy, validate_strategy_config, StrategyValidationContext,
};
use telemetry::telemetry;
use uuid::Uuid;

const BPS_DENOMINATOR: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct ReplayEngine {
    pool: PgPool,
    source: String,
}

#[derive(Debug, Clone)]
pub struct BacktestExecution {
    pub result: BacktestResult,
    pub trades: Vec<BacktestTrade>,
    pub equity_curve: Vec<BacktestEquityPoint>,
}

#[derive(Debug, Clone)]
pub struct StrategyExperimentExecution {
    pub result: StrategyExperimentResult,
    pub runs: Vec<StrategyExperimentRun>,
}

#[derive(Debug, Clone)]
pub struct StrategyMultiTimeframeExperimentExecution {
    pub result: StrategyMultiTimeframeExperimentResult,
    pub experiments: Vec<StrategyExperimentExecution>,
}

#[derive(Debug, Clone)]
pub struct StrategyWalkForwardExecution {
    pub result: StrategyWalkForwardResult,
    pub windows: Vec<StrategyWalkForwardWindowResult>,
}

#[derive(Debug, Clone)]
struct SimulationState {
    cash: Decimal,
    peak_equity: Decimal,
    position: Option<BacktestPosition>,
    trades: Vec<BacktestTrade>,
    equity_curve: Vec<BacktestEquityPoint>,
    fee_paid: Decimal,
    slippage_cost: Decimal,
}

impl ReplayEngine {
    pub fn new(pool: PgPool, source: impl Into<String>) -> Self {
        Self {
            pool,
            source: source.into(),
        }
    }

    pub async fn run_backtest(&self, request: BacktestRequest) -> Result<BacktestExecution> {
        request.validate()?;
        let started_at = std::time::Instant::now();
        let metrics = telemetry();

        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let run_id = Uuid::new_v4();
        let created_at = Utc::now();
        let config = request.config();

        insert_backtest_run(
            &self.pool,
            run_id,
            &request,
            &config,
            created_at,
            ReplayRunStatus::Running,
            Some(correlation_id),
        )
        .await
        .context("failed to insert backtest run")?;

        self.emit_event(
            correlation_id,
            "replay.backtest.started",
            json!({
                "run_id": run_id,
                "strategy_id": request.strategy_id,
                "symbol": request.symbol,
                "timeframe": request.timeframe,
                "start_time": request.start_time,
                "end_time": request.end_time,
            }),
        )
        .await?;

        let execution = match self
            .execute(run_id, created_at, correlation_id, &request)
            .await
        {
            Ok(execution) => execution,
            Err(err) => {
                metrics.inc_backtest_run(
                    request.strategy_id.as_str(),
                    request.symbol.as_str(),
                    "failed",
                );
                let failed = failure_result(run_id, created_at, Some(correlation_id), &request);
                let _ = update_backtest_run_completed(&self.pool, &failed, &config).await;
                let _ = self
                    .emit_event(
                        correlation_id,
                        "replay.backtest.failed",
                        json!({
                            "run_id": run_id,
                            "strategy_id": request.strategy_id,
                            "symbol": request.symbol,
                            "error": err.to_string(),
                        }),
                    )
                    .await;
                return Err(err);
            }
        };

        update_backtest_run_completed(&self.pool, &execution.result, &config)
            .await
            .context("failed to update completed backtest run")?;

        for trade in &execution.trades {
            insert_backtest_trade(&self.pool, trade)
                .await
                .context("failed to insert backtest trade")?;
        }

        insert_backtest_equity_points(&self.pool, &execution.equity_curve)
            .await
            .context("failed to insert backtest equity curve")?;

        self.emit_event(
            correlation_id,
            "replay.backtest.completed",
            json!({
                "run_id": execution.result.run_id,
                "strategy_id": execution.result.strategy_id,
                "symbol": execution.result.symbol,
                "trade_count": execution.result.trade_count,
                "pnl": execution.result.pnl,
                "pnl_pct": execution.result.pnl_pct,
            }),
        )
        .await?;
        metrics.inc_backtest_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            execution.result.status.as_str(),
        );
        metrics.observe_backtest_duration(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            started_at.elapsed(),
        );
        metrics.add_backtest_trades(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            execution.result.trade_count.max(0) as u64,
        );

        Ok(execution)
    }

    pub async fn run_strategy_experiment(
        &self,
        request: StrategyExperimentRequest,
    ) -> Result<StrategyExperimentExecution> {
        request.validate()?;

        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let created_at = Utc::now();
        let (base_config, symbol) = self
            .load_strategy_experiment_context(&request.strategy_id, &request.symbol)
            .await?;
        let candles = get_closed_candles_range(
            &self.pool,
            &symbol,
            parse_strategy_timeframe(&request.timeframe)?,
            request.start_time,
            request.end_time,
        )
        .await
        .context("failed to load closed candles range")?;
        let execution = build_strategy_experiment_execution(
            &base_config,
            symbol,
            request,
            candles,
            created_at,
            correlation_id,
            None,
        )?;

        insert_strategy_experiment(&self.pool, &execution.result)
            .await
            .context("failed to insert strategy experiment")?;
        insert_strategy_experiment_runs(&self.pool, &execution.runs)
            .await
            .context("failed to insert strategy experiment runs")?;

        Ok(execution)
    }

    pub async fn run_multi_timeframe_strategy_experiment(
        &self,
        request: StrategyMultiTimeframeExperimentRequest,
    ) -> Result<StrategyMultiTimeframeExperimentExecution> {
        request.validate()?;

        let experiment_group_id = Uuid::new_v4();
        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let created_at = Utc::now();
        let (base_config, symbol) = self
            .load_strategy_experiment_context(&request.strategy_id, &request.symbol)
            .await?;

        let mut experiments = Vec::new();
        let mut timeframe_comparisons = Vec::new();
        let mut global_entries = Vec::new();

        for timeframe in &request.timeframes {
            let single_request = request.single_timeframe_request(timeframe.clone());
            let parsed_timeframe = parse_strategy_timeframe(timeframe)?;
            let candles = get_closed_candles_range(
                &self.pool,
                &symbol,
                parsed_timeframe,
                request.start_time,
                request.end_time,
            )
            .await
            .context("failed to load closed candles range")?;
            let candle_count = candles.len() as i32;
            let required_candles = required_candles_for_request(&single_request) as i32;

            if candle_count < required_candles {
                let skipped_reason = format!(
                    "insufficient_candle_coverage: required={required_candles} actual={candle_count}"
                );
                let result = skipped_strategy_experiment_result(
                    experiment_group_id,
                    &single_request,
                    created_at,
                    correlation_id,
                    candle_count,
                    skipped_reason.clone(),
                );
                insert_strategy_experiment(&self.pool, &result)
                    .await
                    .context("failed to insert skipped strategy experiment")?;
                timeframe_comparisons.push(timeframe_comparison_from_result(&result));
                experiments.push(StrategyExperimentExecution {
                    result,
                    runs: Vec::new(),
                });
                continue;
            }

            let execution = build_strategy_experiment_execution(
                &base_config,
                symbol.clone(),
                single_request,
                candles,
                created_at,
                correlation_id,
                Some(experiment_group_id),
            )?;

            insert_strategy_experiment(&self.pool, &execution.result)
                .await
                .context("failed to insert strategy experiment")?;
            insert_strategy_experiment_runs(&self.pool, &execution.runs)
                .await
                .context("failed to insert strategy experiment runs")?;

            let timeframe_candle_count = execution.result.candle_count.unwrap_or_default();
            let timeframe_required = required_candles_from_runs(&execution.runs);
            global_entries.extend(execution.runs.iter().cloned().map(|run| {
                global_ranking_entry(
                    execution.result.timeframe.clone(),
                    execution.result.experiment_id,
                    timeframe_candle_count,
                    timeframe_required,
                    run,
                )
            }));
            timeframe_comparisons.push(timeframe_comparison_from_result(&execution.result));
            experiments.push(execution);
        }

        let global_ranking = build_global_ranking(&mut global_entries);
        let warnings = multi_timeframe_warnings(&timeframe_comparisons, &global_ranking);
        let status = if global_ranking.ranked_runs.is_empty() {
            StrategyExperimentStatus::Failed
        } else {
            StrategyExperimentStatus::Completed
        };

        let result = StrategyMultiTimeframeExperimentResult {
            experiment_group_id,
            strategy_id: request.strategy_id,
            symbol: request.symbol,
            requested_timeframes: request.timeframes,
            start_time: request.start_time,
            end_time: request.end_time,
            initial_capital: request.initial_capital,
            fee_bps: request.fee_bps,
            slippage_bps: request.slippage_bps,
            max_signal_age_ms: request.max_signal_age_ms,
            max_runs: request.max_runs,
            status,
            timeframe_comparisons,
            global_ranking,
            warnings,
            created_at,
            correlation_id: Some(correlation_id),
        };

        Ok(StrategyMultiTimeframeExperimentExecution {
            result,
            experiments,
        })
    }

    pub async fn run_strategy_walk_forward(
        &self,
        mut request: StrategyWalkForwardRequest,
    ) -> Result<StrategyWalkForwardExecution> {
        request = self.resolve_strategy_walk_forward_request(request).await?;
        request.validate()?;

        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let created_at = Utc::now();
        let walk_forward_id = Uuid::new_v4();
        let (base_config, symbol) = self
            .load_strategy_experiment_context(&request.strategy_id, &request.symbol)
            .await?;
        let timeframe = parse_strategy_timeframe(&request.timeframe)?;
        let candles = get_closed_candles_range(
            &self.pool,
            &symbol,
            timeframe,
            request.start_time,
            request.end_time,
        )
        .await
        .context("failed to load closed candles range")?;

        let execution = build_strategy_walk_forward_execution(
            walk_forward_id,
            created_at,
            correlation_id,
            &base_config,
            request.clone(),
            candles,
        )?;

        insert_strategy_walk_forward_run(&self.pool, &request, &execution.result)
            .await
            .context("failed to insert strategy walk-forward run")?;
        insert_strategy_walk_forward_windows(&self.pool, &execution.windows)
            .await
            .context("failed to insert strategy walk-forward windows")?;

        Ok(execution)
    }

    async fn resolve_strategy_walk_forward_request(
        &self,
        mut request: StrategyWalkForwardRequest,
    ) -> Result<StrategyWalkForwardRequest> {
        if let Some(experiment_run_id) = request.experiment_run_id {
            let run = get_strategy_experiment_run(&self.pool, experiment_run_id)
                .await
                .context("failed to load strategy experiment run for walk-forward")?
                .ok_or_else(|| anyhow!("strategy experiment run was not found"))?;
            let candidate = serde_json::from_value::<aegis_core::StrategyExperimentCandidate>(
                run.candidate_config,
            )
            .context("failed to decode strategy experiment run candidate config")?;
            request.candidate_config = StrategyWalkForwardCandidate {
                lookback_candles: candidate.lookback_candles,
                trend_lookback_candles: candidate.trend_lookback_candles,
                momentum_lookback_candles: candidate.momentum_lookback_candles,
                breakout_lookback_candles: candidate.breakout_lookback_candles,
                holding_candles: candidate.holding_candles,
                stop_loss_pct: candidate.stop_loss_pct,
                take_profit_pct: candidate.take_profit_pct,
                max_signal_age_ms: candidate.max_signal_age_ms,
            };
        } else if let Some(config) = request.config.clone() {
            request.candidate_config = strategy_walk_forward_candidate_from_config(&config)?;
        }

        Ok(request)
    }

    async fn execute(
        &self,
        run_id: Uuid,
        created_at: DateTime<Utc>,
        correlation_id: Uuid,
        request: &BacktestRequest,
    ) -> Result<BacktestExecution> {
        let strategy_id: StrategyId = request
            .strategy_id
            .parse()
            .context("invalid strategy_id for backtest")?;
        let symbol = Symbol::new(request.symbol.clone()).context("invalid symbol for backtest")?;
        let timeframe: CandleInterval = request
            .timeframe
            .parse()
            .context("invalid timeframe for backtest")?;
        let config_record = get_strategy_status(&self.pool, strategy_id)
            .await?
            .map(|status| status.config)
            .ok_or_else(|| anyhow!("persisted strategy config not found"))?;
        let strategy_config = if let Some(override_request) = &request.strategy_config_override {
            let validation = validate_strategy_config(
                override_request,
                &StrategyValidationContext {
                    supported_symbols: vec![symbol.clone()],
                    max_position_notional: Some(
                        aegis_core::RiskConfig::default().max_position_notional,
                    ),
                },
            );
            validation
                .normalized_config
                .ok_or_else(|| anyhow!("invalid strategy_config_override for backtest"))?
        } else {
            strategy_config_from_record(&config_record)
                .context("invalid persisted strategy config")?
        };

        let candles = get_closed_candles_range(
            &self.pool,
            &symbol,
            timeframe,
            request.start_time,
            request.end_time,
        )
        .await
        .context("failed to load closed candles range")?;

        if candles.is_empty() {
            return Ok(BacktestExecution {
                result: failure_result(run_id, created_at, Some(correlation_id), request),
                trades: Vec::new(),
                equity_curve: Vec::new(),
            });
        }

        Ok(simulate_backtest(
            run_id,
            created_at,
            correlation_id,
            request,
            &strategy_config,
            candles,
        )?)
    }

    async fn emit_event(
        &self,
        correlation_id: Uuid,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        insert_system_event(
            &self.pool,
            &EventEnvelope::new(event_type, correlation_id, self.source.clone(), payload),
        )
        .await?;
        Ok(())
    }

    async fn load_strategy_experiment_context(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> Result<(StrategyConfig, Symbol)> {
        let strategy_id: StrategyId = strategy_id
            .parse()
            .context("invalid strategy_id for strategy experiment")?;
        let symbol =
            Symbol::new(symbol.to_string()).context("invalid symbol for strategy experiment")?;
        let config_record = get_strategy_status(&self.pool, strategy_id)
            .await?
            .map(|status| status.config)
            .ok_or_else(|| anyhow!("persisted strategy config not found"))?;
        let base_config = strategy_config_from_record(&config_record)
            .context("invalid persisted strategy config")?;

        Ok((base_config, symbol))
    }
}

pub async fn fetch_backtest_result(pool: &PgPool, run_id: Uuid) -> Result<Option<BacktestResult>> {
    Ok(get_backtest_run(pool, run_id)
        .await?
        .as_ref()
        .map(backtest_result_from_record)
        .transpose()?)
}

pub fn simulate_backtest(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Uuid,
    request: &BacktestRequest,
    strategy_config: &StrategyConfig,
    candles: Vec<Candle>,
) -> Result<BacktestExecution> {
    if candles.is_empty() {
        return Ok(BacktestExecution {
            result: failure_result(run_id, created_at, Some(correlation_id), request),
            trades: Vec::new(),
            equity_curve: Vec::new(),
        });
    }

    let strategy_id: StrategyId = request.strategy_id.parse()?;
    let symbol = Symbol::new(request.symbol.clone())?;
    let backtest_config = request.config();
    let mut state = SimulationState {
        cash: request.initial_capital,
        peak_equity: request.initial_capital,
        position: None,
        trades: Vec::new(),
        equity_curve: Vec::new(),
        fee_paid: Decimal::ZERO,
        slippage_cost: Decimal::ZERO,
    };

    for index in 0..candles.len() {
        let candle = &candles[index];

        if let Some(position) = state.position.take() {
            state.position = evaluate_exit(
                run_id,
                created_at,
                &backtest_config,
                position,
                candle,
                &mut state,
                &request.strategy_id,
                &request.symbol,
            )?;
        }

        if state.position.is_none() && index + 1 < candles.len() {
            let evaluation = evaluate_strategy(StrategyEvaluationContext {
                correlation_id,
                strategy_id,
                symbol: symbol.clone(),
                config: strategy_config.clone(),
                candles: candles[..=index].to_vec(),
                evaluated_at: candle.close_time,
            })?;

            if let Some(signal) = evaluation.signal {
                if signal.side == aegis_core::SignalSide::Buy {
                    let next_candle = &candles[index + 1];
                    state.position = maybe_open_position(
                        &backtest_config,
                        strategy_config,
                        next_candle,
                        state.cash,
                    )?;
                }
            }
        }

        record_equity(run_id, candle.close_time, candle.close, &mut state)?;
    }

    if let (Some(position), Some(last_candle)) = (state.position.take(), candles.last()) {
        close_position(
            run_id,
            created_at,
            &backtest_config,
            position,
            last_candle.close_time,
            last_candle.close,
            "replay_end",
            &mut state,
            &request.strategy_id,
            &request.symbol,
        )?;
        record_equity(
            run_id,
            last_candle.close_time,
            last_candle.close,
            &mut state,
        )?;
    }

    let result = build_result(
        run_id,
        created_at,
        Some(correlation_id),
        request,
        &state.trades,
        &state.equity_curve,
        state.fee_paid,
        state.slippage_cost,
        state.cash,
    );

    Ok(BacktestExecution {
        result,
        trades: state.trades,
        equity_curve: state.equity_curve,
    })
}

fn experiment_strategy_override(
    base_config: &StrategyConfig,
    request: &StrategyExperimentRequest,
    candidate: &StrategyExperimentCandidate,
) -> StrategyConfigUpdateRequest {
    StrategyConfigUpdateRequest {
        strategy_id: request.strategy_id.clone(),
        enabled: base_config.enabled,
        mode: base_config.mode,
        symbols: vec![request.symbol.clone()],
        timeframe: request.timeframe.clone(),
        suggested_notional: base_config.suggested_notional,
        max_signal_age_ms: candidate
            .max_signal_age_ms
            .unwrap_or(base_config.max_signal_age_ms),
        cooldown_seconds: base_config.cooldown_seconds,
        lookback_candles: candidate.lookback_candles,
        trend_lookback_candles: candidate
            .trend_lookback_candles
            .or(Some(candidate.lookback_candles))
            .filter(|_| request.strategy_id == StrategyId::TrendFilterMomentumV1.as_str())
            .or(base_config.trend_lookback_candles),
        momentum_lookback_candles: candidate
            .momentum_lookback_candles
            .filter(|_| request.strategy_id == StrategyId::TrendFilterMomentumV1.as_str())
            .or(base_config.momentum_lookback_candles),
        breakout_lookback_candles: candidate
            .breakout_lookback_candles
            .or(Some(candidate.lookback_candles))
            .filter(|_| request.strategy_id == StrategyId::VolatilityBreakoutV2.as_str())
            .or(base_config.breakout_lookback_candles),
        confidence_floor: base_config.confidence_floor,
        stop_loss_pct: candidate.stop_loss_pct.or(base_config.stop_loss_pct),
        take_profit_pct: candidate.take_profit_pct.or(base_config.take_profit_pct),
        holding_candles: candidate.holding_candles.or(base_config.holding_candles),
        notes: base_config.notes.clone(),
    }
}

fn build_strategy_experiment_execution(
    base_config: &StrategyConfig,
    symbol: Symbol,
    request: StrategyExperimentRequest,
    candles: Vec<Candle>,
    created_at: DateTime<Utc>,
    correlation_id: Uuid,
    experiment_group_id: Option<Uuid>,
) -> Result<StrategyExperimentExecution> {
    let experiment_id = Uuid::new_v4();
    let candidates = request.candidates();
    if candidates.is_empty() {
        return Err(anyhow!(
            "strategy experiment requires at least one candidate"
        ));
    }

    let candle_count = candles.len() as i32;
    let mut runs = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let override_request = experiment_strategy_override(base_config, &request, &candidate);
        let validation = validate_strategy_config(
            &override_request,
            &StrategyValidationContext {
                supported_symbols: vec![symbol.clone()],
                max_position_notional: Some(
                    aegis_core::RiskConfig::default().max_position_notional,
                ),
            },
        );
        let strategy_config = validation
            .normalized_config
            .ok_or_else(|| anyhow!("invalid strategy experiment candidate override"))?;
        let run_request = BacktestRequest {
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            initial_capital: request.initial_capital,
            risk_config_id: None,
            risk_config: None,
            fee_bps: request.fee_bps,
            slippage_bps: request.slippage_bps,
            correlation_id: Some(correlation_id),
            holding_candles: candidate
                .holding_candles
                .or(strategy_config.holding_candles),
            strategy_config_override: Some(override_request),
        };
        let execution = simulate_backtest(
            Uuid::new_v4(),
            created_at,
            correlation_id,
            &run_request,
            &strategy_config,
            candles.clone(),
        )?;
        runs.push(strategy_experiment_run_from_backtest(
            experiment_id,
            created_at,
            request.initial_capital,
            candidate,
            execution.result,
            candle_count,
        ));
    }

    rank_strategy_experiment_runs(&mut runs);
    let comparison = strategy_experiment_comparison(&runs);
    let best_run = comparison
        .best_run_id
        .and_then(|id| runs.iter().find(|run| run.id == id).cloned());
    let worst_run = comparison
        .worst_run_id
        .and_then(|id| runs.iter().find(|run| run.id == id).cloned());
    let warnings = aggregate_experiment_warnings(&runs);
    let status = if runs
        .iter()
        .all(|run| run.status == StrategyExperimentStatus::Completed)
    {
        StrategyExperimentStatus::Completed
    } else {
        StrategyExperimentStatus::Failed
    };

    let result = StrategyExperimentResult {
        experiment_id,
        experiment_group_id,
        strategy_id: request.strategy_id,
        symbol: request.symbol,
        timeframe: request.timeframe,
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        fee_bps: request.fee_bps,
        slippage_bps: request.slippage_bps,
        max_signal_age_ms: request.max_signal_age_ms,
        max_runs: request.max_runs,
        status,
        run_count: runs.len() as i32,
        comparison,
        best_run,
        worst_run,
        candle_count: Some(candle_count),
        warnings,
        skipped_reason: None,
        created_at,
        correlation_id: Some(correlation_id),
    };

    Ok(StrategyExperimentExecution { result, runs })
}

fn skipped_strategy_experiment_result(
    experiment_group_id: Uuid,
    request: &StrategyExperimentRequest,
    created_at: DateTime<Utc>,
    correlation_id: Uuid,
    candle_count: i32,
    skipped_reason: String,
) -> StrategyExperimentResult {
    StrategyExperimentResult {
        experiment_id: Uuid::new_v4(),
        experiment_group_id: Some(experiment_group_id),
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        fee_bps: request.fee_bps,
        slippage_bps: request.slippage_bps,
        max_signal_age_ms: request.max_signal_age_ms,
        max_runs: request.max_runs,
        status: StrategyExperimentStatus::Failed,
        run_count: 0,
        comparison: strategy_experiment_comparison(&[]),
        best_run: None,
        worst_run: None,
        candle_count: Some(candle_count),
        warnings: vec!["insufficient_data".to_string()],
        skipped_reason: Some(skipped_reason),
        created_at,
        correlation_id: Some(correlation_id),
    }
}

fn strategy_experiment_run_from_backtest(
    experiment_id: Uuid,
    created_at: DateTime<Utc>,
    initial_capital: Decimal,
    candidate: StrategyExperimentCandidate,
    result: BacktestResult,
    candle_count: i32,
) -> StrategyExperimentRun {
    let fee_slippage_drag_pct =
        calculate_fee_slippage_drag_pct(initial_capital, result.fee_paid, result.slippage_cost);
    let warnings = experiment_warnings(
        result.trade_count,
        result.pnl,
        result.max_drawdown_pct,
        fee_slippage_drag_pct,
        candle_count,
    );
    let mut run = StrategyExperimentRun {
        id: Uuid::new_v4(),
        experiment_id,
        rank: 0,
        candidate,
        final_equity: result.final_equity,
        pnl: result.pnl,
        pnl_pct: result.pnl_pct,
        max_drawdown_pct: result.max_drawdown_pct,
        win_rate: result.win_rate,
        trade_count: result.trade_count,
        fee_paid: result.fee_paid,
        slippage_cost: result.slippage_cost,
        fee_slippage_drag_pct,
        score: Decimal::ZERO,
        status: match result.status {
            ReplayRunStatus::Completed => StrategyExperimentStatus::Completed,
            ReplayRunStatus::Failed => StrategyExperimentStatus::Failed,
            ReplayRunStatus::Pending => StrategyExperimentStatus::Pending,
            ReplayRunStatus::Running => StrategyExperimentStatus::Running,
        },
        warnings,
        created_at,
    };
    run.score = calculate_strategy_experiment_score(&run, candle_count);
    run
}

pub fn calculate_fee_slippage_drag_pct(
    initial_capital: Decimal,
    fee_paid: Decimal,
    slippage_cost: Decimal,
) -> Decimal {
    if initial_capital <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((fee_paid + slippage_cost) / initial_capital) * Decimal::new(100, 0)
}

pub fn calculate_strategy_experiment_score(
    run: &StrategyExperimentRun,
    candle_count: i32,
) -> Decimal {
    let trade_penalty = overtrading_penalty(run.trade_count, candle_count)
        + low_trade_count_penalty(run.trade_count);
    let insufficient_data_penalty = insufficient_data_penalty(
        candle_count,
        required_candles_for_candidate(&run.candidate) as i32,
    );

    run.pnl_pct - (run.max_drawdown_pct / Decimal::new(2, 0)) + (run.win_rate / Decimal::new(10, 0))
        - run.fee_slippage_drag_pct
        - trade_penalty
        - insufficient_data_penalty
}

pub fn rank_strategy_experiment_runs(runs: &mut [StrategyExperimentRun]) {
    runs.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.pnl_pct.cmp(&left.pnl_pct))
            .then_with(|| left.max_drawdown_pct.cmp(&right.max_drawdown_pct))
            .then_with(|| right.win_rate.cmp(&left.win_rate))
            .then_with(|| left.fee_slippage_drag_pct.cmp(&right.fee_slippage_drag_pct))
            .then_with(|| right.trade_count.cmp(&left.trade_count))
            .then_with(|| left.id.cmp(&right.id))
    });

    for (index, run) in runs.iter_mut().enumerate() {
        run.rank = index as i32 + 1;
    }
}

fn strategy_experiment_comparison(runs: &[StrategyExperimentRun]) -> StrategyExperimentComparison {
    StrategyExperimentComparison {
        ranking_metric: StrategyExperimentMetric::RiskAdjustedScore,
        best_run_id: runs.first().map(|run| run.id),
        worst_run_id: runs.last().map(|run| run.id),
        ranked_run_ids: runs.iter().map(|run| run.id).collect(),
    }
}

fn experiment_warnings(
    trade_count: i32,
    pnl: Decimal,
    max_drawdown_pct: Decimal,
    fee_slippage_drag_pct: Decimal,
    candle_count: i32,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if overtrading_penalty(trade_count, candle_count) > Decimal::ZERO {
        warnings.push("overtrading_warning".to_string());
    }
    if pnl < Decimal::ZERO && fee_slippage_drag_pct > Decimal::ZERO {
        warnings.push("negative_after_fees".to_string());
    }
    if max_drawdown_pct >= Decimal::new(15, 0) {
        warnings.push("high_drawdown".to_string());
    }
    if trade_count > 0 && trade_count < 3 {
        warnings.push("too_few_trades".to_string());
    }

    warnings
}

fn timeframe_comparison_from_result(
    result: &StrategyExperimentResult,
) -> StrategyTimeframeComparison {
    StrategyTimeframeComparison {
        candidate: StrategyTimeframeCandidate {
            timeframe: result.timeframe.clone(),
            candle_count: result.candle_count.unwrap_or_default(),
            required_candles: result
                .best_run
                .as_ref()
                .map(|run| required_candles_for_candidate(&run.candidate) as i32)
                .unwrap_or_default(),
        },
        experiment_id: Some(result.experiment_id),
        status: result.status,
        run_count: result.run_count,
        best_run: result.best_run.clone(),
        skipped_reason: result.skipped_reason.clone(),
        warnings: result.warnings.clone(),
    }
}

fn global_ranking_entry(
    timeframe: String,
    experiment_id: Uuid,
    candle_count: i32,
    required_candles: i32,
    run: StrategyExperimentRun,
) -> StrategyExperimentGlobalRankingEntry {
    let insufficient_data_penalty = insufficient_data_penalty(candle_count, required_candles);
    let overtrading_penalty = overtrading_penalty(run.trade_count, candle_count);
    let mut warnings = run.warnings.clone();
    if insufficient_data_penalty > Decimal::ZERO
        && !warnings.iter().any(|item| item == "thin_sample")
    {
        warnings.push("thin_sample".to_string());
    }

    StrategyExperimentGlobalRankingEntry {
        timeframe,
        experiment_id,
        candle_count,
        required_candles,
        insufficient_data_penalty,
        overtrading_penalty,
        run,
        warnings,
    }
}

fn build_global_ranking(
    entries: &mut [StrategyExperimentGlobalRankingEntry],
) -> StrategyExperimentGlobalRanking {
    entries.sort_by(|left, right| {
        right
            .run
            .score
            .cmp(&left.run.score)
            .then_with(|| right.run.pnl_pct.cmp(&left.run.pnl_pct))
            .then_with(|| left.run.max_drawdown_pct.cmp(&right.run.max_drawdown_pct))
            .then_with(|| right.run.win_rate.cmp(&left.run.win_rate))
            .then_with(|| {
                left.run
                    .fee_slippage_drag_pct
                    .cmp(&right.run.fee_slippage_drag_pct)
            })
            .then_with(|| right.run.trade_count.cmp(&left.run.trade_count))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
            .then_with(|| left.run.id.cmp(&right.run.id))
    });

    StrategyExperimentGlobalRanking {
        ranking_metric: StrategyExperimentMetric::RiskAdjustedScore,
        best_run_id: entries.first().map(|entry| entry.run.id),
        ranked_runs: entries.to_vec(),
    }
}

fn multi_timeframe_warnings(
    comparisons: &[StrategyTimeframeComparison],
    global_ranking: &StrategyExperimentGlobalRanking,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if comparisons
        .iter()
        .any(|comparison| comparison.skipped_reason.is_some())
    {
        warnings.push("skipped_timeframes_present".to_string());
    }
    if global_ranking.ranked_runs.iter().any(|entry| {
        entry
            .warnings
            .iter()
            .any(|warning| warning == "overtrading_warning")
    }) {
        warnings.push("overtrading_candidates_present".to_string());
    }

    warnings
}

fn aggregate_experiment_warnings(runs: &[StrategyExperimentRun]) -> Vec<String> {
    let mut warnings = Vec::new();
    for warning in runs.iter().flat_map(|run| run.warnings.iter()) {
        if !warnings.iter().any(|item| item == warning) {
            warnings.push(warning.clone());
        }
    }
    warnings
}

fn walk_forward_strategy_override(
    base_config: &StrategyConfig,
    request: &StrategyWalkForwardRequest,
) -> StrategyConfigUpdateRequest {
    StrategyConfigUpdateRequest {
        strategy_id: request.strategy_id.clone(),
        enabled: base_config.enabled,
        mode: base_config.mode,
        symbols: vec![request.symbol.clone()],
        timeframe: request.timeframe.clone(),
        suggested_notional: base_config.suggested_notional,
        max_signal_age_ms: request
            .candidate_config
            .max_signal_age_ms
            .unwrap_or(base_config.max_signal_age_ms),
        cooldown_seconds: base_config.cooldown_seconds,
        lookback_candles: request.candidate_config.lookback_candles,
        trend_lookback_candles: request
            .candidate_config
            .trend_lookback_candles
            .or(Some(request.candidate_config.lookback_candles))
            .filter(|_| request.strategy_id == StrategyId::TrendFilterMomentumV1.as_str())
            .or(base_config.trend_lookback_candles),
        momentum_lookback_candles: request
            .candidate_config
            .momentum_lookback_candles
            .filter(|_| request.strategy_id == StrategyId::TrendFilterMomentumV1.as_str())
            .or(base_config.momentum_lookback_candles),
        breakout_lookback_candles: request
            .candidate_config
            .breakout_lookback_candles
            .or(Some(request.candidate_config.lookback_candles))
            .filter(|_| request.strategy_id == StrategyId::VolatilityBreakoutV2.as_str())
            .or(base_config.breakout_lookback_candles),
        confidence_floor: base_config.confidence_floor,
        stop_loss_pct: request
            .candidate_config
            .stop_loss_pct
            .or(base_config.stop_loss_pct),
        take_profit_pct: request
            .candidate_config
            .take_profit_pct
            .or(base_config.take_profit_pct),
        holding_candles: request
            .candidate_config
            .holding_candles
            .or(base_config.holding_candles),
        notes: base_config.notes.clone(),
    }
}

fn strategy_walk_forward_candidate_from_config(
    config: &serde_json::Value,
) -> Result<StrategyWalkForwardCandidate> {
    let params = config.get("params").unwrap_or(config);
    let read_u32 = |keys: &[&str]| -> Option<u32> {
        keys.iter()
            .find_map(|key| params.get(*key).and_then(|value| value.as_u64()))
            .and_then(|value| u32::try_from(value).ok())
    };
    let read_i64 = |keys: &[&str]| -> Option<i64> {
        keys.iter()
            .find_map(|key| params.get(*key).and_then(|value| value.as_i64()))
    };
    let read_decimal = |keys: &[&str]| -> Result<Option<Decimal>> {
        keys.iter()
            .find_map(|key| params.get(*key))
            .map(|value| match value {
                serde_json::Value::String(raw) => raw
                    .parse::<Decimal>()
                    .context("invalid decimal strategy walk-forward config value"),
                serde_json::Value::Number(number) => number
                    .to_string()
                    .parse::<Decimal>()
                    .context("invalid decimal strategy walk-forward config value"),
                _ => Err(anyhow!(
                    "invalid decimal strategy walk-forward config value"
                )),
            })
            .transpose()
    };

    Ok(StrategyWalkForwardCandidate {
        lookback_candles: read_u32(&["lookback_candles"]).unwrap_or_else(|| {
            read_u32(&[
                "trend_lookback_candles",
                "trend_lookback",
                "breakout_lookback_candles",
                "breakout_lookback",
            ])
            .unwrap_or(0)
        }),
        trend_lookback_candles: read_u32(&["trend_lookback_candles", "trend_lookback"]),
        momentum_lookback_candles: read_u32(&["momentum_lookback_candles", "momentum_lookback"]),
        breakout_lookback_candles: read_u32(&["breakout_lookback_candles", "breakout_lookback"]),
        holding_candles: read_u32(&["holding_candles", "holding"]),
        stop_loss_pct: read_decimal(&["stop_loss_pct"])?,
        take_profit_pct: read_decimal(&["take_profit_pct"])?,
        max_signal_age_ms: read_i64(&["max_signal_age_ms"]),
    })
}

fn build_strategy_walk_forward_execution(
    walk_forward_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Uuid,
    base_config: &StrategyConfig,
    request: StrategyWalkForwardRequest,
    candles: Vec<Candle>,
) -> Result<StrategyWalkForwardExecution> {
    let windows = generate_walk_forward_windows(&request)?;
    let override_request = walk_forward_strategy_override(base_config, &request);
    let normalized_symbol =
        Symbol::new(request.symbol.clone()).context("invalid symbol for strategy walk-forward")?;
    let validation = validate_strategy_config(
        &override_request,
        &StrategyValidationContext {
            supported_symbols: vec![normalized_symbol],
            max_position_notional: Some(aegis_core::RiskConfig::default().max_position_notional),
        },
    );
    let strategy_config = validation
        .normalized_config
        .ok_or_else(|| anyhow!("invalid strategy walk-forward candidate override"))?;
    let timeframe = parse_strategy_timeframe(&request.timeframe)?;

    let mut window_results = Vec::with_capacity(windows.len());
    for window in windows {
        let expected_candles = timeframe
            .candles_between(window.test_start, window.test_end)
            .context("invalid strategy walk-forward test window")?;
        let test_candles = candles_for_range(&candles, window.test_start, window.test_end);
        let required_candles =
            required_candles_for_walk_forward_candidate(&request.candidate_config) as i32;

        if test_candles.len() as i32 != expected_candles
            || (test_candles.len() as i32) < required_candles
        {
            let actual = test_candles.len() as i32;
            let skip_reason = format!(
                "insufficient_candle_coverage: expected={expected_candles} actual={actual} required={required_candles}"
            );
            window_results.push(StrategyWalkForwardWindowResult {
                id: Uuid::new_v4(),
                walk_forward_id,
                window,
                status: StrategyWalkForwardStatus::Skipped,
                skip_reason: Some(skip_reason.clone()),
                trade_count: 0,
                pnl: Decimal::ZERO,
                pnl_pct: Decimal::ZERO,
                max_drawdown_pct: Decimal::ZERO,
                win_rate: Decimal::ZERO,
                fee_paid: Decimal::ZERO,
                slippage_cost: Decimal::ZERO,
                result: json!({ "status": "SKIPPED", "skip_reason": skip_reason }),
                created_at,
            });
            continue;
        }

        let run_request = BacktestRequest {
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            start_time: window.test_start,
            end_time: window.test_end,
            initial_capital: request.initial_capital,
            risk_config_id: None,
            risk_config: None,
            fee_bps: request.fee_bps,
            slippage_bps: request.slippage_bps,
            correlation_id: Some(correlation_id),
            holding_candles: request
                .candidate_config
                .holding_candles
                .or(strategy_config.holding_candles),
            strategy_config_override: Some(override_request.clone()),
        };
        let execution = simulate_backtest(
            Uuid::new_v4(),
            created_at,
            correlation_id,
            &run_request,
            &strategy_config,
            test_candles,
        )?;
        let result = serde_json::to_value(&execution.result)?;
        window_results.push(StrategyWalkForwardWindowResult {
            id: Uuid::new_v4(),
            walk_forward_id,
            window,
            status: match execution.result.status {
                ReplayRunStatus::Completed => StrategyWalkForwardStatus::Completed,
                ReplayRunStatus::Failed => StrategyWalkForwardStatus::Failed,
                ReplayRunStatus::Pending => StrategyWalkForwardStatus::Pending,
                ReplayRunStatus::Running => StrategyWalkForwardStatus::Running,
            },
            skip_reason: None,
            trade_count: execution.result.trade_count,
            pnl: execution.result.pnl,
            pnl_pct: execution.result.pnl_pct,
            max_drawdown_pct: execution.result.max_drawdown_pct,
            win_rate: execution.result.win_rate,
            fee_paid: execution.result.fee_paid,
            slippage_cost: execution.result.slippage_cost,
            result,
            created_at,
        });
    }

    let result = build_strategy_walk_forward_result(
        walk_forward_id,
        created_at,
        Some(correlation_id),
        &request,
        &window_results,
    );

    Ok(StrategyWalkForwardExecution {
        result,
        windows: window_results,
    })
}

pub fn generate_walk_forward_windows(
    request: &StrategyWalkForwardRequest,
) -> Result<Vec<StrategyWalkForwardWindow>> {
    let train_size = Duration::hours(request.window_train_size_hours);
    let test_size = Duration::hours(request.window_test_size_hours);
    let step_size = Duration::hours(request.step_size_hours);
    let mut windows = Vec::new();
    let mut train_start = request.start_time;
    let mut window_index = 0;

    loop {
        let train_end = train_start + train_size;
        let test_start = train_end;
        let test_end = test_start + test_size;
        if test_end > request.end_time {
            break;
        }

        windows.push(StrategyWalkForwardWindow {
            window_index,
            train_start,
            train_end,
            test_start,
            test_end,
        });
        window_index += 1;
        train_start += step_size;
    }

    Ok(windows)
}

fn candles_for_range(
    candles: &[Candle],
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Vec<Candle> {
    candles
        .iter()
        .filter(|candle| candle.open_time >= start_time && candle.open_time < end_time)
        .cloned()
        .collect()
}

fn required_candles_for_walk_forward_candidate(candidate: &StrategyWalkForwardCandidate) -> usize {
    let max_lookback = [
        candidate.lookback_candles,
        candidate.trend_lookback_candles.unwrap_or(0),
        candidate.momentum_lookback_candles.unwrap_or(0),
        candidate.breakout_lookback_candles.unwrap_or(0),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    max_lookback as usize + candidate.holding_candles.unwrap_or(0) as usize + 2
}

fn build_strategy_walk_forward_result(
    walk_forward_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    request: &StrategyWalkForwardRequest,
    windows: &[StrategyWalkForwardWindowResult],
) -> StrategyWalkForwardResult {
    let completed = windows
        .iter()
        .filter(|window| window.status == StrategyWalkForwardStatus::Completed)
        .collect::<Vec<_>>();
    let failed_windows = windows
        .iter()
        .filter(|window| window.status == StrategyWalkForwardStatus::Failed)
        .count() as i32;
    let skipped_windows = windows
        .iter()
        .filter(|window| window.status == StrategyWalkForwardStatus::Skipped)
        .count() as i32;
    let profitable_test_windows = completed
        .iter()
        .filter(|window| window.pnl_pct > Decimal::ZERO)
        .count() as i32;
    let losing_test_windows = completed
        .iter()
        .filter(|window| window.pnl_pct < Decimal::ZERO)
        .count() as i32;
    let completed_windows = completed.len() as i32;
    let total_windows = windows.len() as i32;
    let avg_test_pnl_pct = average_decimal(completed.iter().map(|window| window.pnl_pct));
    let median_test_pnl_pct =
        median_decimal(completed.iter().map(|window| window.pnl_pct).collect());
    let worst_test_pnl_pct = completed
        .iter()
        .map(|window| window.pnl_pct)
        .min()
        .unwrap_or(Decimal::ZERO);
    let best_test_pnl_pct = completed
        .iter()
        .map(|window| window.pnl_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let avg_max_drawdown_pct =
        average_decimal(completed.iter().map(|window| window.max_drawdown_pct));
    let max_drawdown_pct = completed
        .iter()
        .map(|window| window.max_drawdown_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let avg_trade_count = average_decimal(
        completed
            .iter()
            .map(|window| Decimal::from(window.trade_count)),
    );
    let robustness_summary =
        build_walk_forward_robustness_summary(windows, &completed, request.initial_capital);
    let robustness_score = calculate_walk_forward_robustness_score(
        completed_windows,
        profitable_test_windows,
        losing_test_windows,
        avg_test_pnl_pct,
        worst_test_pnl_pct,
        avg_max_drawdown_pct,
        skipped_windows,
        total_windows,
        &robustness_summary,
    );
    let min_required_test_windows = request.min_required_test_windows.unwrap_or(1) as i32;
    let status = if completed_windows == 0 && skipped_windows > 0 {
        StrategyWalkForwardStatus::Skipped
    } else if completed_windows < min_required_test_windows {
        StrategyWalkForwardStatus::Failed
    } else {
        StrategyWalkForwardStatus::Completed
    };
    let robustness_status = classify_walk_forward_robustness(
        status,
        completed_windows,
        min_required_test_windows,
        profitable_test_windows,
        losing_test_windows,
        avg_test_pnl_pct,
        worst_test_pnl_pct,
        skipped_windows,
        total_windows,
        robustness_score,
        &robustness_summary,
    );
    let recommendation = walk_forward_recommendation(robustness_status);

    StrategyWalkForwardResult {
        walk_forward_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        total_windows,
        completed_windows,
        failed_windows,
        skipped_windows,
        profitable_test_windows,
        profitable_windows: profitable_test_windows,
        losing_test_windows,
        losing_windows: losing_test_windows,
        avg_test_pnl_pct,
        avg_pnl_pct: avg_test_pnl_pct,
        median_test_pnl_pct,
        median_pnl_pct: median_test_pnl_pct,
        worst_test_pnl_pct,
        worst_pnl_pct: worst_test_pnl_pct,
        best_test_pnl_pct,
        best_pnl_pct: best_test_pnl_pct,
        avg_max_drawdown_pct,
        max_drawdown_pct,
        avg_trade_count,
        robustness_score,
        consistency_score: robustness_score,
        status,
        robustness_status,
        robustness_summary,
        recommendation,
        warnings: Vec::new(),
        created_at,
        correlation_id,
    }
}

fn build_walk_forward_robustness_summary(
    windows: &[StrategyWalkForwardWindowResult],
    completed: &[&StrategyWalkForwardWindowResult],
    initial_capital: Decimal,
) -> StrategyWalkForwardRobustnessSummary {
    let profitable_window_pct = if completed.is_empty() {
        Decimal::ZERO
    } else {
        (Decimal::from(
            completed
                .iter()
                .filter(|window| window.pnl_pct > Decimal::ZERO)
                .count() as i32,
        ) / Decimal::from(completed.len() as i32))
            * Decimal::new(100, 0)
    };
    let total_trade_count = completed.iter().map(|window| window.trade_count).sum();
    let avg_trades_per_completed_window = if completed.is_empty() {
        Decimal::ZERO
    } else {
        Decimal::from(total_trade_count) / Decimal::from(completed.len() as i32)
    };
    let avg_fee_slippage_drag_pct = if completed.is_empty() {
        Decimal::ZERO
    } else {
        average_decimal(completed.iter().map(|window| {
            calculate_fee_slippage_drag_pct(initial_capital, window.fee_paid, window.slippage_cost)
        }))
    };
    let skipped_window_pct = if windows.is_empty() {
        Decimal::ZERO
    } else {
        (Decimal::from(
            windows
                .iter()
                .filter(|window| window.status == StrategyWalkForwardStatus::Skipped)
                .count() as i32,
        ) / Decimal::from(windows.len() as i32))
            * Decimal::new(100, 0)
    };
    let total_positive_pnl = completed
        .iter()
        .filter(|window| window.pnl_pct > Decimal::ZERO)
        .fold(Decimal::ZERO, |acc, window| acc + window.pnl_pct);
    let dominant_winner_share_pct = completed
        .iter()
        .map(|window| window.pnl_pct.max(Decimal::ZERO))
        .max()
        .filter(|_| total_positive_pnl > Decimal::ZERO)
        .map(|best| (best / total_positive_pnl) * Decimal::new(100, 0))
        .unwrap_or(Decimal::ZERO);

    StrategyWalkForwardRobustnessSummary {
        profitable_window_pct,
        total_trade_count,
        avg_trades_per_completed_window,
        avg_fee_slippage_drag_pct,
        skipped_window_pct,
        dominant_winner_share_pct,
        recommendation: StrategyWalkForwardRecommendation {
            action: "REVIEW".to_string(),
            reason: "Review walk-forward robustness before candidate acceptance.".to_string(),
        },
    }
}

fn classify_walk_forward_robustness(
    status: StrategyWalkForwardStatus,
    completed_windows: i32,
    min_required_test_windows: i32,
    profitable_test_windows: i32,
    losing_test_windows: i32,
    avg_test_pnl_pct: Decimal,
    worst_test_pnl_pct: Decimal,
    skipped_windows: i32,
    total_windows: i32,
    robustness_score: Decimal,
    summary: &StrategyWalkForwardRobustnessSummary,
) -> StrategyWalkForwardRobustnessStatus {
    if status == StrategyWalkForwardStatus::Failed {
        return StrategyWalkForwardRobustnessStatus::Failed;
    }
    if completed_windows < min_required_test_windows
        || completed_windows == 0
        || (total_windows > 0 && skipped_windows == total_windows)
    {
        return StrategyWalkForwardRobustnessStatus::InsufficientData;
    }
    if profitable_test_windows == 1
        && losing_test_windows >= 2
        && summary.dominant_winner_share_pct >= Decimal::new(55, 0)
    {
        return StrategyWalkForwardRobustnessStatus::OverfitRisk;
    }
    if profitable_test_windows <= losing_test_windows
        || avg_test_pnl_pct <= Decimal::ZERO
        || worst_test_pnl_pct < Decimal::new(-2, 0)
    {
        return StrategyWalkForwardRobustnessStatus::OverfitRisk;
    }
    if robustness_score >= Decimal::new(60, 0)
        && summary.profitable_window_pct >= Decimal::new(65, 0)
        && summary.dominant_winner_share_pct < Decimal::new(55, 0)
    {
        return StrategyWalkForwardRobustnessStatus::Robust;
    }
    StrategyWalkForwardRobustnessStatus::Weak
}

fn walk_forward_recommendation(
    robustness_status: StrategyWalkForwardRobustnessStatus,
) -> StrategyWalkForwardRecommendation {
    match robustness_status {
        StrategyWalkForwardRobustnessStatus::Robust => StrategyWalkForwardRecommendation {
            action: "REVIEW_FOR_CANDIDATE".to_string(),
            reason: "Multiple out-of-sample windows were profitable without concentrated winners."
                .to_string(),
        },
        StrategyWalkForwardRobustnessStatus::Weak => StrategyWalkForwardRecommendation {
            action: "KEEP_RESEARCHING".to_string(),
            reason: "The candidate has mixed walk-forward evidence and needs more data."
                .to_string(),
        },
        StrategyWalkForwardRobustnessStatus::OverfitRisk => StrategyWalkForwardRecommendation {
            action: "DO_NOT_ACCEPT".to_string(),
            reason: "Walk-forward results are dominated by weak or inconsistent test windows."
                .to_string(),
        },
        StrategyWalkForwardRobustnessStatus::InsufficientData => {
            StrategyWalkForwardRecommendation {
                action: "COLLECT_MORE_DATA".to_string(),
                reason: "Not enough completed out-of-sample windows were available.".to_string(),
            }
        }
        StrategyWalkForwardRobustnessStatus::Failed => StrategyWalkForwardRecommendation {
            action: "INVESTIGATE".to_string(),
            reason: "The walk-forward run failed validation or execution.".to_string(),
        },
    }
}

pub fn calculate_walk_forward_robustness_score(
    completed_windows: i32,
    profitable_test_windows: i32,
    losing_test_windows: i32,
    avg_test_pnl_pct: Decimal,
    worst_test_pnl_pct: Decimal,
    avg_max_drawdown_pct: Decimal,
    skipped_windows: i32,
    total_windows: i32,
    summary: &StrategyWalkForwardRobustnessSummary,
) -> Decimal {
    let profitable_pct = if completed_windows <= 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(profitable_test_windows) / Decimal::from(completed_windows))
            * Decimal::new(100, 0)
    };
    let skip_penalty = if total_windows <= 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(skipped_windows) / Decimal::from(total_windows)) * Decimal::new(20, 0)
    };
    let low_trade_penalty = if summary.total_trade_count < completed_windows.max(1) * 2 {
        Decimal::new(12, 0)
    } else if summary.total_trade_count < completed_windows.max(1) * 4 {
        Decimal::new(5, 0)
    } else {
        Decimal::ZERO
    };
    let loser_penalty = if losing_test_windows > profitable_test_windows {
        Decimal::from(losing_test_windows - profitable_test_windows) * Decimal::new(3, 0)
    } else {
        Decimal::ZERO
    };
    let concentration_penalty = if summary.dominant_winner_share_pct >= Decimal::new(70, 0)
        && losing_test_windows >= profitable_test_windows
    {
        Decimal::new(12, 0)
    } else if summary.dominant_winner_share_pct >= Decimal::new(55, 0) {
        Decimal::new(5, 0)
    } else {
        Decimal::ZERO
    };

    profitable_pct + (avg_test_pnl_pct * Decimal::new(3, 0)) + worst_test_pnl_pct
        - (avg_max_drawdown_pct * Decimal::new(2, 0))
        - (summary.avg_fee_slippage_drag_pct * Decimal::new(2, 0))
        - skip_penalty
        - low_trade_penalty
        - loser_penalty
        - concentration_penalty
}

fn average_decimal<I>(values: I) -> Decimal
where
    I: Iterator<Item = Decimal>,
{
    let mut total = Decimal::ZERO;
    let mut count = 0i32;
    for value in values {
        total += value;
        count += 1;
    }

    if count == 0 {
        Decimal::ZERO
    } else {
        total / Decimal::from(count)
    }
}

pub fn median_decimal(mut values: Vec<Decimal>) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }

    values.sort();
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / Decimal::new(2, 0)
    } else {
        values[mid]
    }
}

fn parse_strategy_timeframe(value: &str) -> Result<CandleInterval> {
    value
        .parse()
        .context("invalid timeframe for strategy experiment")
}

fn required_candles_for_request(request: &StrategyExperimentRequest) -> usize {
    let max_lookback = request
        .lookback_candidates
        .iter()
        .copied()
        .max()
        .unwrap_or(1) as usize;
    let max_holding = request
        .holding_candles_candidates
        .as_ref()
        .and_then(|values| values.iter().copied().max())
        .unwrap_or(0) as usize;

    max_lookback.saturating_add(max_holding).saturating_add(2)
}

fn required_candles_for_candidate(candidate: &StrategyExperimentCandidate) -> usize {
    candidate.lookback_candles as usize + candidate.holding_candles.unwrap_or(0) as usize + 2
}

fn required_candles_from_runs(runs: &[StrategyExperimentRun]) -> i32 {
    runs.iter()
        .map(|run| required_candles_for_candidate(&run.candidate) as i32)
        .max()
        .unwrap_or_default()
}

fn insufficient_data_penalty(candle_count: i32, required_candles: i32) -> Decimal {
    if candle_count <= required_candles {
        return Decimal::new(25, 0);
    }

    let surplus = candle_count - required_candles;
    if surplus < required_candles {
        Decimal::new(3, 0)
    } else if surplus < required_candles.saturating_mul(2) {
        Decimal::new(1, 0)
    } else {
        Decimal::ZERO
    }
}

fn overtrading_penalty(trade_count: i32, candle_count: i32) -> Decimal {
    if trade_count <= 0 || candle_count <= 0 {
        return Decimal::ZERO;
    }

    if trade_count >= 200 {
        return Decimal::from((trade_count - 200) / 10) + Decimal::new(2, 0);
    }

    let activity_pct =
        (Decimal::from(trade_count) / Decimal::from(candle_count)) * Decimal::new(100, 0);
    if activity_pct >= Decimal::new(25, 0) {
        Decimal::new(3, 0)
    } else {
        Decimal::ZERO
    }
}

fn low_trade_count_penalty(trade_count: i32) -> Decimal {
    if trade_count > 0 && trade_count < 3 {
        Decimal::new(5, 0)
    } else {
        Decimal::ZERO
    }
}

fn maybe_open_position(
    backtest_config: &aegis_core::BacktestConfig,
    strategy_config: &StrategyConfig,
    next_candle: &Candle,
    available_cash: Decimal,
) -> Result<Option<BacktestPosition>> {
    let fee_multiplier = Decimal::ONE + (backtest_config.fee_bps / Decimal::from(BPS_DENOMINATOR));
    let max_notional = available_cash / fee_multiplier;
    let target_notional = strategy_config.suggested_notional.min(max_notional);
    if target_notional <= Decimal::ZERO {
        return Ok(None);
    }

    let entry_price = apply_entry_slippage(next_candle.open, backtest_config.slippage_bps);
    let quantity = target_notional / entry_price;
    if quantity <= Decimal::ZERO {
        return Ok(None);
    }

    let executed_notional = quantity * entry_price;
    let entry_slippage_cost = (entry_price - next_candle.open) * quantity;
    let entry_fee = fee_amount(executed_notional, backtest_config.fee_bps);

    Ok(Some(BacktestPosition {
        side: Side::Buy,
        entry_time: next_candle.open_time,
        entry_price,
        quantity,
        notional: executed_notional,
        fee_paid: entry_fee,
        slippage_cost: entry_slippage_cost,
        remaining_holding_candles: backtest_config.holding_candles,
        stop_loss_price: strategy_config
            .stop_loss_pct
            .map(|pct| entry_price * (Decimal::ONE - pct)),
        take_profit_price: strategy_config
            .take_profit_pct
            .map(|pct| entry_price * (Decimal::ONE + pct)),
    }))
}

fn evaluate_exit(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    backtest_config: &aegis_core::BacktestConfig,
    mut position: BacktestPosition,
    candle: &Candle,
    state: &mut SimulationState,
    strategy_id: &str,
    symbol: &str,
) -> Result<Option<BacktestPosition>> {
    if let Some(stop_loss_price) = position.stop_loss_price {
        if candle.low <= stop_loss_price {
            close_position(
                run_id,
                created_at,
                backtest_config,
                position,
                candle.close_time,
                stop_loss_price,
                "stop_loss",
                state,
                strategy_id,
                symbol,
            )?;
            return Ok(None);
        }
    }

    if let Some(take_profit_price) = position.take_profit_price {
        if candle.high >= take_profit_price {
            close_position(
                run_id,
                created_at,
                backtest_config,
                position,
                candle.close_time,
                take_profit_price,
                "take_profit",
                state,
                strategy_id,
                symbol,
            )?;
            return Ok(None);
        }
    }

    if position.remaining_holding_candles > 0 {
        position.remaining_holding_candles -= 1;
    }

    if position.remaining_holding_candles == 0 {
        close_position(
            run_id,
            created_at,
            backtest_config,
            position,
            candle.close_time,
            candle.close,
            "holding_period",
            state,
            strategy_id,
            symbol,
        )?;
        return Ok(None);
    }

    Ok(Some(position))
}

fn close_position(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    backtest_config: &aegis_core::BacktestConfig,
    position: BacktestPosition,
    exit_time: DateTime<Utc>,
    reference_exit_price: Decimal,
    reason: &str,
    state: &mut SimulationState,
    strategy_id: &str,
    symbol: &str,
) -> Result<()> {
    let exit_price = apply_exit_slippage(reference_exit_price, backtest_config.slippage_bps);
    let exit_notional = position.quantity * exit_price;
    let exit_fee = fee_amount(exit_notional, backtest_config.fee_bps);
    let exit_slippage_cost = (reference_exit_price - exit_price) * position.quantity;
    let realized_pnl = exit_notional - exit_fee - position.notional - position.fee_paid;

    state.cash -= position.notional + position.fee_paid;
    state.cash += exit_notional - exit_fee;
    state.fee_paid += position.fee_paid + exit_fee;
    state.slippage_cost += position.slippage_cost + exit_slippage_cost;
    state.trades.push(BacktestTrade {
        id: deterministic_trade_id(run_id, position.entry_time, exit_time),
        run_id,
        strategy_id: strategy_id.to_string(),
        symbol: symbol.to_string(),
        side: position.side,
        entry_time: position.entry_time,
        entry_price: position.entry_price,
        exit_time: Some(exit_time),
        exit_price: Some(exit_price),
        quantity: position.quantity,
        notional: position.notional,
        fee_paid: position.fee_paid + exit_fee,
        slippage_cost: position.slippage_cost + exit_slippage_cost,
        realized_pnl,
        reason: reason.to_string(),
        created_at,
    });
    Ok(())
}

fn record_equity(
    run_id: Uuid,
    timestamp: DateTime<Utc>,
    mark_price: Decimal,
    state: &mut SimulationState,
) -> Result<()> {
    let equity = if let Some(position) = state.position.as_ref() {
        state.cash + (position.quantity * mark_price) - position.fee_paid - position.notional
    } else {
        state.cash
    };
    if equity > state.peak_equity {
        state.peak_equity = equity;
    }
    let drawdown_pct = if state.peak_equity > Decimal::ZERO {
        ((state.peak_equity - equity) / state.peak_equity) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    let point = BacktestEquityPoint {
        id: deterministic_point_id(run_id, timestamp),
        run_id,
        timestamp,
        equity,
        drawdown_pct,
    };
    if let Some(existing) = state
        .equity_curve
        .iter_mut()
        .find(|existing| existing.timestamp == timestamp)
    {
        *existing = point;
    } else {
        state.equity_curve.push(point);
    }
    Ok(())
}

fn build_result(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    request: &BacktestRequest,
    trades: &[BacktestTrade],
    equity_curve: &[BacktestEquityPoint],
    fee_paid: Decimal,
    slippage_cost: Decimal,
    final_equity: Decimal,
) -> BacktestResult {
    let winning_trades = trades
        .iter()
        .filter(|trade| trade.realized_pnl > Decimal::ZERO)
        .count() as i32;
    let losing_trades = trades
        .iter()
        .filter(|trade| trade.realized_pnl < Decimal::ZERO)
        .count() as i32;
    let trade_count = trades.len() as i32;
    let pnl = final_equity - request.initial_capital;
    let pnl_pct = if request.initial_capital > Decimal::ZERO {
        (pnl / request.initial_capital) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    let max_drawdown_pct = equity_curve
        .iter()
        .map(|point| point.drawdown_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let win_rate = if trade_count > 0 {
        (Decimal::from(winning_trades) / Decimal::from(trade_count)) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    let avg_win = average_trade_pnl(
        trades
            .iter()
            .filter(|trade| trade.realized_pnl > Decimal::ZERO)
            .map(|trade| trade.realized_pnl),
        winning_trades,
    );
    let avg_loss = average_trade_pnl(
        trades
            .iter()
            .filter(|trade| trade.realized_pnl < Decimal::ZERO)
            .map(|trade| trade.realized_pnl),
        losing_trades,
    );

    BacktestResult {
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        final_equity,
        pnl,
        pnl_pct,
        max_drawdown_pct,
        win_rate,
        trade_count,
        winning_trades,
        losing_trades,
        avg_win,
        avg_loss,
        fee_paid,
        slippage_cost,
        status: ReplayRunStatus::Completed,
        created_at,
        correlation_id,
    }
}

fn failure_result(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    request: &BacktestRequest,
) -> BacktestResult {
    BacktestResult {
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        final_equity: request.initial_capital,
        pnl: Decimal::ZERO,
        pnl_pct: Decimal::ZERO,
        max_drawdown_pct: Decimal::ZERO,
        win_rate: Decimal::ZERO,
        trade_count: 0,
        winning_trades: 0,
        losing_trades: 0,
        avg_win: Decimal::ZERO,
        avg_loss: Decimal::ZERO,
        fee_paid: Decimal::ZERO,
        slippage_cost: Decimal::ZERO,
        status: ReplayRunStatus::Failed,
        created_at,
        correlation_id,
    }
}

fn average_trade_pnl<I>(values: I, count: i32) -> Decimal
where
    I: Iterator<Item = Decimal>,
{
    if count == 0 {
        return Decimal::ZERO;
    }

    values.fold(Decimal::ZERO, |acc, value| acc + value) / Decimal::from(count)
}

fn fee_amount(notional: Decimal, fee_bps: Decimal) -> Decimal {
    notional * fee_bps / Decimal::from(BPS_DENOMINATOR)
}

fn apply_entry_slippage(price: Decimal, slippage_bps: Decimal) -> Decimal {
    price * (Decimal::ONE + slippage_bps / Decimal::from(BPS_DENOMINATOR))
}

fn apply_exit_slippage(price: Decimal, slippage_bps: Decimal) -> Decimal {
    price * (Decimal::ONE - slippage_bps / Decimal::from(BPS_DENOMINATOR))
}

fn deterministic_trade_id(
    run_id: Uuid,
    entry_time: DateTime<Utc>,
    exit_time: DateTime<Utc>,
) -> Uuid {
    let seed =
        (entry_time.timestamp_millis() as u128) << 64 | (exit_time.timestamp_millis() as u128);
    Uuid::from_u128(run_id.as_u128() ^ seed)
}

fn deterministic_point_id(run_id: Uuid, timestamp: DateTime<Utc>) -> Uuid {
    let seed = timestamp.timestamp_millis() as u128;
    Uuid::from_u128(run_id.as_u128() ^ seed)
}

#[cfg(test)]
mod tests {
    use super::{
        build_global_ranking, build_strategy_experiment_execution,
        build_strategy_walk_forward_execution, calculate_fee_slippage_drag_pct,
        calculate_strategy_experiment_score, calculate_walk_forward_robustness_score,
        experiment_strategy_override, experiment_warnings, generate_walk_forward_windows,
        global_ranking_entry, median_decimal, rank_strategy_experiment_runs, simulate_backtest,
        skipped_strategy_experiment_result, timeframe_comparison_from_result,
    };
    use aegis_core::{
        BacktestRequest, Candle, CandleInterval, MarketDataSource, StrategyConfig,
        StrategyExperimentRequest, StrategyExperimentRun, StrategyExperimentStatus, StrategyId,
        StrategyMode, StrategyWalkForwardCandidate, StrategyWalkForwardRequest,
        StrategyWalkForwardRobustnessSummary, Symbol,
    };
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_request() -> BacktestRequest {
        BacktestRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 1, 1, 0, 0).unwrap(),
            initial_capital: Decimal::new(1_000_000, 0),
            risk_config_id: None,
            risk_config: None,
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            correlation_id: Some(Uuid::from_u128(0xabc)),
            holding_candles: Some(3),
            strategy_config_override: None,
        }
    }

    fn sample_strategy_config() -> StrategyConfig {
        StrategyConfig {
            strategy_id: StrategyId::MomentumV1,
            enabled: true,
            mode: StrategyMode::Paper,
            symbols: vec![Symbol::new("BTCUSDT").unwrap()],
            timeframe: CandleInterval::OneMinute,
            suggested_notional: Decimal::new(100_000, 0),
            max_signal_age_ms: 180_000,
            cooldown_seconds: 900,
            lookback_candles: 3,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: None,
        }
    }

    fn candle(index: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        let open_time =
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap() + Duration::minutes(index);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + Duration::minutes(1),
            open: Decimal::new(open, 0),
            high: Decimal::new(high, 0),
            low: Decimal::new(low, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1_000, 0)),
            trade_count: 1,
            is_closed: true,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    fn trending_candles() -> Vec<Candle> {
        vec![
            candle(0, 100, 101, 99, 100),
            candle(1, 100, 102, 99, 101),
            candle(2, 101, 103, 100, 102),
            candle(3, 102, 104, 101, 103),
            candle(4, 103, 106, 102, 105),
            candle(5, 105, 107, 104, 106),
            candle(6, 106, 108, 105, 107),
            candle(7, 107, 109, 106, 108),
        ]
    }

    fn long_trending_candles(count: i64) -> Vec<Candle> {
        (0..count)
            .map(|index| candle(index, 100 + index, 101 + index, 99 + index, 100 + index))
            .collect()
    }

    fn sample_experiment_request() -> StrategyExperimentRequest {
        StrategyExperimentRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            initial_capital: Decimal::new(1_000_000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            lookback_candidates: vec![3, 5, 10],
            trend_lookback_candidates: None,
            momentum_lookback_candidates: None,
            breakout_lookback_candidates: None,
            holding_candles_candidates: Some(vec![3, 5]),
            stop_loss_pct_candidates: None,
            take_profit_pct_candidates: None,
            max_signal_age_ms: Some(180_000),
            max_runs: Some(3),
            correlation_id: None,
        }
    }

    fn trend_filter_strategy_config() -> StrategyConfig {
        StrategyConfig {
            strategy_id: StrategyId::TrendFilterMomentumV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: vec![Symbol::new("BTCUSDT").unwrap()],
            timeframe: CandleInterval::FiveMinutes,
            suggested_notional: Decimal::new(100_000, 0),
            max_signal_age_ms: 900_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: Some(20),
            momentum_lookback_candles: Some(3),
            breakout_lookback_candles: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: None,
        }
    }

    fn trend_filter_request() -> BacktestRequest {
        BacktestRequest {
            strategy_id: "trend_filter_momentum_v1".to_string(),
            timeframe: "5m".to_string(),
            strategy_config_override: None,
            ..sample_request()
        }
    }

    fn trend_filter_experiment_request() -> StrategyExperimentRequest {
        StrategyExperimentRequest {
            strategy_id: "trend_filter_momentum_v1".to_string(),
            timeframe: "5m".to_string(),
            lookback_candidates: vec![10, 20],
            trend_lookback_candidates: Some(vec![10, 20]),
            momentum_lookback_candidates: Some(vec![2, 3]),
            breakout_lookback_candidates: None,
            max_runs: Some(4),
            ..sample_experiment_request()
        }
    }

    fn sample_experiment_run(
        id: Uuid,
        pnl_pct: i64,
        max_drawdown_pct: i64,
        trade_count: i32,
        win_rate: i64,
        fee_slippage_drag_pct: i64,
    ) -> StrategyExperimentRun {
        let mut run = StrategyExperimentRun {
            id,
            experiment_id: Uuid::from_u128(0x1000),
            rank: 0,
            candidate: aegis_core::StrategyExperimentCandidate {
                lookback_candles: 3,
                trend_lookback_candles: None,
                momentum_lookback_candles: None,
                breakout_lookback_candles: None,
                holding_candles: Some(3),
                stop_loss_pct: None,
                take_profit_pct: None,
                max_signal_age_ms: Some(180_000),
            },
            final_equity: Decimal::new(1_000_000 + pnl_pct * 10_000, 0),
            pnl: Decimal::new(pnl_pct * 10_000, 0),
            pnl_pct: Decimal::new(pnl_pct, 0),
            max_drawdown_pct: Decimal::new(max_drawdown_pct, 0),
            win_rate: Decimal::new(win_rate, 0),
            trade_count,
            fee_paid: Decimal::new(500, 0),
            slippage_cost: Decimal::new(250, 0),
            fee_slippage_drag_pct: Decimal::new(fee_slippage_drag_pct, 0),
            score: Decimal::ZERO,
            status: StrategyExperimentStatus::Completed,
            warnings: Vec::new(),
            created_at: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
        };
        run.score = calculate_strategy_experiment_score(&run, 40);
        run
    }

    fn sample_walk_forward_request() -> StrategyWalkForwardRequest {
        StrategyWalkForwardRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1h".to_string(),
            config: None,
            experiment_run_id: None,
            start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 6, 0, 0, 0).unwrap(),
            window_train_size_hours: 24,
            window_test_size_hours: 12,
            step_size_hours: 12,
            initial_capital: Decimal::new(1_000_000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            candidate_config: StrategyWalkForwardCandidate {
                lookback_candles: 5,
                trend_lookback_candles: None,
                momentum_lookback_candles: None,
                breakout_lookback_candles: None,
                holding_candles: Some(3),
                stop_loss_pct: None,
                take_profit_pct: None,
                max_signal_age_ms: Some(180_000),
            },
            min_required_test_windows: Some(2),
            correlation_id: None,
        }
    }

    #[test]
    fn same_candle_input_produces_same_result() {
        let request = sample_request();
        let config = sample_strategy_config();
        let candles = trending_candles();
        let run_id = Uuid::from_u128(1);
        let created_at = Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap();

        let first = simulate_backtest(
            run_id,
            created_at,
            Uuid::from_u128(2),
            &request,
            &config,
            candles.clone(),
        )
        .unwrap();
        let second = simulate_backtest(
            run_id,
            created_at,
            Uuid::from_u128(2),
            &request,
            &config,
            candles,
        )
        .unwrap();

        assert_eq!(first.result, second.result);
        assert_eq!(first.trades, second.trades);
        assert_eq!(first.equity_curve, second.equity_curve);
    }

    #[test]
    fn no_candles_returns_failed_status_cleanly() {
        let request = sample_request();
        let config = sample_strategy_config();
        let result = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &request,
            &config,
            Vec::new(),
        );

        let execution = result.unwrap();
        assert!(execution.trades.is_empty());
        assert_eq!(execution.result.status, aegis_core::ReplayRunStatus::Failed);
    }

    #[test]
    fn buy_signal_opens_simulated_position() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(!execution.trades.is_empty());
        assert_eq!(execution.trades[0].side, aegis_core::Side::Buy);
    }

    #[test]
    fn trend_filter_momentum_runs_in_backtest_path() {
        let execution = simulate_backtest(
            Uuid::from_u128(0x501),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(0x502),
            &trend_filter_request(),
            &trend_filter_strategy_config(),
            long_trending_candles(40),
        )
        .unwrap();

        assert_eq!(execution.result.strategy_id, "trend_filter_momentum_v1");
        assert!(!execution.trades.is_empty());
    }

    #[test]
    fn trend_filter_experiment_evaluates_candidates() {
        let execution = build_strategy_experiment_execution(
            &trend_filter_strategy_config(),
            Symbol::new("BTCUSDT").unwrap(),
            trend_filter_experiment_request(),
            long_trending_candles(80),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(0x503),
            None,
        )
        .unwrap();

        assert_eq!(execution.result.strategy_id, "trend_filter_momentum_v1");
        assert!(!execution.runs.is_empty());
        assert!(execution.runs.iter().any(|run| {
            run.candidate.trend_lookback_candles == Some(10)
                && run.candidate.momentum_lookback_candles == Some(2)
        }));
    }

    #[test]
    fn holding_period_closes_position() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert_eq!(execution.trades[0].reason, "holding_period");
    }

    #[test]
    fn fee_reduces_pnl() {
        let mut fee_request = sample_request();
        fee_request.fee_bps = Decimal::new(10, 0);
        let no_fee = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();
        let with_fee = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &fee_request,
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(with_fee.result.pnl < no_fee.result.pnl);
    }

    #[test]
    fn slippage_reduces_pnl() {
        let mut slip_request = sample_request();
        slip_request.slippage_bps = Decimal::new(10, 0);
        let no_slip = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();
        let with_slip = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &slip_request,
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(with_slip.result.pnl < no_slip.result.pnl);
    }

    #[test]
    fn max_drawdown_calculation_is_correct() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            vec![
                candle(0, 100, 101, 99, 100),
                candle(1, 100, 102, 99, 101),
                candle(2, 101, 103, 100, 102),
                candle(3, 102, 104, 101, 103),
                candle(4, 103, 104, 80, 81),
                candle(5, 81, 82, 80, 81),
                candle(6, 81, 82, 80, 81),
                candle(7, 81, 82, 80, 81),
            ],
        )
        .unwrap();

        assert!(execution.result.max_drawdown_pct > Decimal::ZERO);
    }

    #[test]
    fn win_rate_calculation_is_correct() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert_eq!(execution.result.win_rate, Decimal::new(100, 0));
    }

    #[test]
    fn empty_candidate_list_is_rejected() {
        let mut request = sample_experiment_request();
        request.lookback_candidates.clear();

        assert!(request.validate().is_err());
    }

    #[test]
    fn max_runs_limit_is_enforced() {
        let candidates = sample_experiment_request().candidates();

        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn fee_slippage_drag_is_included() {
        let drag = calculate_fee_slippage_drag_pct(
            Decimal::new(1_000_000, 0),
            Decimal::new(1_000, 0),
            Decimal::new(500, 0),
        );

        assert_eq!(drag, Decimal::new(15, 2));
    }

    #[test]
    fn score_calculation_penalizes_drawdown_and_costs() {
        let run = sample_experiment_run(Uuid::from_u128(1), 12, 4, 10, 60, 1);

        assert_eq!(run.score, Decimal::new(12, 0));
    }

    #[test]
    fn score_penalizes_thin_sample_and_overtrading() {
        let mut run = sample_experiment_run(Uuid::from_u128(9), 12, 4, 12, 60, 1);
        run.score = calculate_strategy_experiment_score(&run, 20);

        assert!(run.score < Decimal::new(15, 0));
    }

    #[test]
    fn experiment_warning_marks_overtrading() {
        let warnings = experiment_warnings(
            12,
            Decimal::new(10, 0),
            Decimal::new(2, 0),
            Decimal::ZERO,
            20,
        );

        assert!(warnings
            .iter()
            .any(|warning| warning == "overtrading_warning"));
    }

    #[test]
    fn global_ranking_orders_across_timeframes() {
        let mut entries = vec![
            global_ranking_entry(
                "1m".to_string(),
                Uuid::from_u128(0x2001),
                120,
                10,
                sample_experiment_run(Uuid::from_u128(1), 8, 4, 20, 55, 1),
            ),
            global_ranking_entry(
                "5m".to_string(),
                Uuid::from_u128(0x2002),
                80,
                10,
                sample_experiment_run(Uuid::from_u128(2), 11, 3, 10, 60, 1),
            ),
        ];

        let ranking = build_global_ranking(&mut entries);

        assert_eq!(ranking.ranked_runs[0].timeframe, "5m");
        assert_eq!(ranking.best_run_id, Some(Uuid::from_u128(2)));
    }

    #[test]
    fn timeframe_comparison_keeps_best_run_and_skip_reason() {
        let result = skipped_strategy_experiment_result(
            Uuid::from_u128(0x3001),
            &sample_experiment_request(),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(0x3002),
            8,
            "insufficient_candle_coverage".to_string(),
        );

        let comparison = timeframe_comparison_from_result(&result);

        assert_eq!(
            comparison.skipped_reason.as_deref(),
            Some("insufficient_candle_coverage")
        );
        assert!(comparison.best_run.is_none());
    }

    #[test]
    fn ranking_orders_highest_score_first() {
        let mut runs = vec![
            sample_experiment_run(Uuid::from_u128(1), 5, 3, 10, 50, 1),
            sample_experiment_run(Uuid::from_u128(2), 12, 4, 10, 60, 1),
            sample_experiment_run(Uuid::from_u128(3), 4, 20, 250, 55, 5),
        ];

        rank_strategy_experiment_runs(&mut runs);

        assert_eq!(runs[0].id, Uuid::from_u128(2));
        assert_eq!(runs[0].rank, 1);
        assert_eq!(runs[2].rank, 3);
    }

    #[test]
    fn experiment_override_does_not_mutate_strategy_config() {
        let base = sample_strategy_config();
        let original = base.clone();
        let candidate = aegis_core::StrategyExperimentCandidate {
            lookback_candles: 10,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            holding_candles: Some(5),
            stop_loss_pct: Some(Decimal::new(2, 0)),
            take_profit_pct: Some(Decimal::new(4, 0)),
            max_signal_age_ms: Some(240_000),
        };

        let override_request =
            experiment_strategy_override(&base, &sample_experiment_request(), &candidate);

        assert_eq!(base, original);
        assert_eq!(override_request.lookback_candles, 10);
        assert_eq!(override_request.holding_candles, Some(5));
    }

    #[test]
    fn walk_forward_window_generation_is_chronological() {
        let windows = generate_walk_forward_windows(&sample_walk_forward_request()).unwrap();

        assert_eq!(windows.len(), 8);
        assert_eq!(windows[0].window_index, 0);
        assert_eq!(
            windows[0].train_start,
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(
            windows[0].test_start,
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap()
        );
        assert_eq!(
            windows[1].train_start,
            Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap()
        );
    }

    #[test]
    fn walk_forward_score_penalizes_many_losing_windows() {
        let good = calculate_walk_forward_robustness_score(
            6,
            5,
            1,
            Decimal::new(2, 0),
            Decimal::new(-1, 0),
            Decimal::new(3, 0),
            0,
            6,
            &StrategyWalkForwardRobustnessSummary {
                profitable_window_pct: Decimal::new(83, 0),
                total_trade_count: 30,
                avg_trades_per_completed_window: Decimal::new(5, 0),
                avg_fee_slippage_drag_pct: Decimal::new(1, 0),
                skipped_window_pct: Decimal::ZERO,
                dominant_winner_share_pct: Decimal::new(25, 0),
                recommendation: aegis_core::StrategyWalkForwardRecommendation::default(),
            },
        );
        let bad = calculate_walk_forward_robustness_score(
            6,
            1,
            5,
            Decimal::new(-1, 0),
            Decimal::new(-8, 0),
            Decimal::new(9, 0),
            1,
            6,
            &StrategyWalkForwardRobustnessSummary {
                profitable_window_pct: Decimal::new(16, 0),
                total_trade_count: 8,
                avg_trades_per_completed_window: Decimal::new(1, 0),
                avg_fee_slippage_drag_pct: Decimal::new(3, 0),
                skipped_window_pct: Decimal::new(16, 0),
                dominant_winner_share_pct: Decimal::new(80, 0),
                recommendation: aegis_core::StrategyWalkForwardRecommendation::default(),
            },
        );

        assert!(bad < good);
    }

    #[test]
    fn walk_forward_skips_window_with_insufficient_data() {
        let request = sample_walk_forward_request();
        let execution = build_strategy_walk_forward_execution(
            Uuid::from_u128(0x5001),
            Utc.with_ymd_and_hms(2026, 5, 6, 0, 0, 0).unwrap(),
            Uuid::from_u128(0x5002),
            &sample_strategy_config(),
            request,
            Vec::new(),
        )
        .unwrap();

        assert_eq!(
            execution.result.skipped_windows,
            execution.result.total_windows
        );
        assert!(execution
            .windows
            .iter()
            .all(|window| window.status == aegis_core::StrategyWalkForwardStatus::Skipped));
    }

    #[test]
    fn median_pnl_is_calculated_for_even_series() {
        let median = median_decimal(vec![
            Decimal::new(-4, 0),
            Decimal::new(2, 0),
            Decimal::new(10, 0),
            Decimal::new(14, 0),
        ]);

        assert_eq!(median, Decimal::new(6, 0));
    }

    #[test]
    fn ranking_is_deterministic_for_equal_scores() {
        let mut runs = vec![
            sample_experiment_run(Uuid::from_u128(2), 10, 4, 10, 60, 1),
            sample_experiment_run(Uuid::from_u128(1), 10, 4, 10, 60, 1),
        ];

        rank_strategy_experiment_runs(&mut runs);
        let first = runs.iter().map(|run| run.id).collect::<Vec<_>>();

        rank_strategy_experiment_runs(&mut runs);
        let second = runs.iter().map(|run| run.id).collect::<Vec<_>>();

        assert_eq!(first, second);
    }
}
